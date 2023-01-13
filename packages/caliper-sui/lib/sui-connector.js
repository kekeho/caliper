'use strict';

const {ConnectorBase, CaliperUtils, ConfigUtil, TxStatus} = require('@hyperledger/caliper-core');
const { JsonRpcProvider, Ed25519Keypair, RawSigner, Base64DataBuffer } = require('@mysten/sui.js');
const assert = require('assert');
const fs = require('fs');
const path = require('path');

const logger = CaliperUtils.getLogger('sui-connector');


class SuiConnector extends ConnectorBase {
    constructor(workerIndex, bcType) {
        super(workerIndex, bcType);

        // Load config
        let configPath = CaliperUtils.resolvePath(
            ConfigUtil.get(ConfigUtil.keys.NetworkConfig)
        );
        this.suiConfig = require(configPath).sui;

        this.srcPath = CaliperUtils.resolvePath(
            ConfigUtil.get(ConfigUtil.keys.Workspace)
        )

        let contractDeployerSeed = new Uint8Array(this.suiConfig.contractDeployerSeed);
        this.contractDeployerKeypair = Ed25519Keypair.fromSeed(contractDeployerSeed);

        let fromSeed = new Uint8Array(this.suiConfig.fromSeed);
        this.fromKeypair = Ed25519Keypair.fromSeed(fromSeed);

        this.suiProvider = new JsonRpcProvider(this.suiConfig.url);
        this.contractDeployerSigner = new RawSigner(this.contractDeployerKeypair, this.suiProvider);
        this.fromSigner = new RawSigner(this.fromKeypair, this.suiProvider);

        this.contracts = {};
        this.gasCoins = Array();

        this.rpcId = 1;
    }


    async init() {
        // Initialization

        // Split coin for gas
        let resp = await this.callRpc('sui_getCoins', {
            owner: this.fromKeypair.getPublicKey().toSuiAddress(),
            coin_type: '0x2::sui::SUI',
        });
        if (resp.hasOwnProperty('error')) {
            CaliperUtils.log('Sui RPC Error: sui_getCoins');
            return -1;
        }

        let coins = resp.result.data;
        let coin = coins[0].coinObjectId;

        let gasBudgerAmount = 65 * this.suiConfig.coins.numberOfCoins;

        let createGasCoinTx = await this.callRpc('sui_transferSui', {
            "signer": this.fromKeypair.getPublicKey().toSuiAddress(),
            sui_object_id: coin,
            gas_budget: 1000,
            recipient: this.fromKeypair.getPublicKey().toSuiAddress(),
            amount: gasBudgerAmount,
        });
        if (createGasCoinTx.hasOwnProperty('error')) {
            CaliperUtils.log('Sui RPC Error: sui_transferSui');
            return -1;
        }

        let createGasCoinResp = await this.fromSigner.signAndExecuteTransaction(new Base64DataBuffer(createGasCoinTx.result.txBytes));

        let split_amounts = Array(this.suiConfig.coins.numberOfCoins);
        for (let i = 0; i < this.suiConfig.coins.numberOfCoins; i++) {
            split_amounts[i] = this.suiConfig.coins.eachAmount;
        }

        let splitCoinTx = await this.callRpc('sui_splitCoin', {
            signer: this.fromKeypair.getPublicKey().toSuiAddress(),
            coin_object_id: coin,
            split_amounts: split_amounts,
            gas_budget: gasBudgerAmount,
            gas: createGasCoinResp.EffectsCert.effects.effects.created[0].reference.objectId,
        });
        if (splitCoinTx.hasOwnProperty('error')) {
            CaliperUtils.log('Sui RPC Error: sui_transferSui');
            return -1;
        }

        let splitted = await this.fromSigner.signAndExecuteTransaction(new Base64DataBuffer(splitCoinTx.result.txBytes));
        
        for (let i = 0; i < splitted.EffectsCert.effects.effects.created.length; i++) {
            const c = splitted.EffectsCert.effects.effects.created[i];
            this.gasCoins.push(c.reference.objectId);
        }
    }

    /**
     * Deploy smart contracts specified in the network configuration file.
     */
    async installSmartContract() {
        logger.info('Creating contracts...');

        for (const key of Object.keys(this.suiConfig.contracts)) {
            const contractConfig = this.suiConfig.contracts[key];
            const contractPath = path.join(this.srcPath, contractConfig.path);
            const modulebytes = fs.readFileSync(contractPath, 'base64');

            let resp_json = await this.callRpc(
                'sui_publish',
                [
                    this.contractDeployerKeypair.getPublicKey().toSuiAddress(),
                    [modulebytes,],
                    null,
                    10000
                ]
            );

            let txbytes = new Base64DataBuffer(resp_json.result.txBytes);
            let publishTxn = await this.contractDeployerSigner.signAndExecuteTransaction(txbytes);  // TODO: RPCに置き換え

            assert.equal(publishTxn.EffectsCert.effects.effects.status.status, "success");

            for (let i = 0; i < publishTxn.EffectsCert.effects.effects.created.length; i++) {
                const element = publishTxn.EffectsCert.effects.effects.created[i];
                
                if (element.owner === "Immutable") {
                    // Package
                    let packageId = element.reference.objectId;
                    if (this.contracts.hasOwnProperty(key) === false) {
                        CaliperUtils.log('a');
                        this.contracts[key] = {
                            packageId: null,
                            initializedObjects: [],
                        }
                    }
                    this.contracts[key].packageId = packageId;
                } else {
                    // Initialized Object
                    let objectId = element.reference.objectId;
                    if (this.contracts.hasOwnProperty(key) === false) {
                        this.contracts[key] = {
                            packageId: null,
                            initializedObjects: [],
                        }
                    }
                    this.contracts[key].initializedObjects.push(objectId);
                }
            }

            logger.info(`Deployed contracts: ${JSON.stringify(this.contracts)}`);
        }
    }


    async prepareWorkerArguments(number) {
        let result = [];

        let gasCoins = Array(number);
        for (let i = 0; i < number; i++) {
            gasCoins[i] = this.gasCoins.slice(i * Math.ceil(this.gasCoins.length / number), (i+1) * Math.ceil(this.gasCoins.length / number));
        }

        for (let i = 0; i < number; i++) {
            result[i] = {
                contracts: this.contracts,
                gasCoins: gasCoins[i],
            };
        }

        return result;
    }


    async getContext(roundIndex, args) {
        let context = {
            clientIndex: this.workerIndex,
            contracts: args.contracts,
            gasCoins: args.gasCoins,
        }

        this.context = context;
        return context;
    }


    async releaseContext() {
        // nothing to do
    }

    /**
     * Send transaction to SUT from Worker
     * @param {} requests
     * @return {Promise<TxStatus>}
     */
    async _sendSingleRequest(requests) {
        if (requests.readOnly === true) {
            return this.readRequest(requests);
        }

        let packageId = this.context.contracts[requests.package].packageId;
        let gas = this.context.gasCoins.pop();

        let args = [];
        for (let i = 0; i < requests.args.length; i++) {
            const element = requests.args[i];
            if (element.hasOwnProperty('createdObject')) {
                args.push('0x' + this.context.contracts[requests.package].initializedObjects[element.createdObject]);
            } else {
                args.push(element);
            }
        }

        let transaction = {
            signer: await this.fromSigner.getAddress(),
            arguments: args,
            function: requests.verb,
            gas: gas,
            gasBudget: 1000,  // TODO: estimate
            module: requests.module,
            packageObjectId: packageId,
            typeArguments: [],
        };
        let resp_json = await this.callRpc("sui_moveCall", transaction);
        assert.equal(resp_json.hasOwnProperty('result'), true);

        let txbytes = new Base64DataBuffer(resp_json.result.txBytes);


        let status = new TxStatus();

        // BCS Serialize
        const INTENT_BYTES = [0, 0, 0];
        let intentMessage = new Uint8Array(INTENT_BYTES.length + txbytes.getLength());
        intentMessage.set(INTENT_BYTES);
        intentMessage.set(txbytes.getData(), INTENT_BYTES.length);
        
        let dataToSign = new Base64DataBuffer(intentMessage);
        let signed = await this.fromSigner.signData(dataToSign);

        let executedResp = await this.callRpc(
            "sui_executeTransaction",
            {
                tx_bytes: txbytes.toString(),
                sig_scheme: signed.signatureScheme,
                signature: signed.signature.toString(),
                pub_key: signed.pubKey.toBase64(),
                request_type: 'WaitForLocalExecution',
            }
        )

        if (executedResp.hasOwnProperty('error')) {
            status.SetStatusFail();
            status.SetErrMsg(executedResp.error.message);
            CaliperUtils.log("Failed", requests, JSON.stringify(executedResp, null, 2));
        } else if (executedResp.result.EffectsCert.effects.effects.status.status == "success") {
            status.SetStatusSuccess();
            status.SetID(executedResp.result.EffectsCert.certificate.transactionDigest);
            status.SetResult(executedResp);
        } else {
            status.SetStatusFail();
            status.SetErrMsg(executedResp.result.EffectsCert.effects.effects.status.status);
            CaliperUtils.log("Failed", requests, JSON.stringify(executedResp, null, 2));
        }

        return status;
    }


    async readRequest(request) {
        // TODO: IMPL
    }


    async callRpc(method, params) {
        const data = {
            "jsonrpc": "2.0",
            "id": this.rpcId++,
            "method": method,
            "params": params
        }
    
        let resp = await fetch(
            this.suiConfig.url,
            {
                'method': 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data),
            }
        )

        return resp.json();
    }
}

module.exports = SuiConnector;
