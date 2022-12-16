'use strict';

const {ConnectorBase, CaliperUtils, ConfigUtil, TxStatus} = require('@hyperledger/caliper-core');
const { JsonRpcProvider, Ed25519Keypair, RawSigner, Base64DataBuffer } = require('@mysten/sui.js');
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

        let seed = new Uint8Array(this.suiConfig.contractDeployerSeed);
        this.contractDeployerKeypair = Ed25519Keypair.fromSeed(seed);

        this.suiProvider = new JsonRpcProvider(this.suiConfig.url);
        this.contractDeployerSigner = new RawSigner(this.contractDeployerKeypair, this.suiProvider);

        this.contracts = {};
    }


    async init() {
        // Initialization
    }

    /**
     * Deploy smart contracts specified in the network configuration file.
     */
    async installSmartContract() {
        logger.info('Creating contracts...');

        let params = [
            this.contractDeployerKeypair.getPublicKey().toSuiAddress(),
            [],
            null,
            10000
        ]

        for (const key of Object.keys(this.suiConfig.contracts)) {
            const contractConfig = this.suiConfig.contracts[key];
            const contractPath = path.join(this.srcPath, contractConfig.path);
            const modulebytes = fs.readFileSync(contractPath, 'base64');
            params[1].push(modulebytes);
        }

        let resp_json = await this.callRpc('sui_publish', params);

        let txbytes = resp_json.result.txBytes;
        let signed = await this.contractDeployerSigner.signData(new Base64DataBuffer(txbytes));

        params = [
            txbytes,
            signed.signatureScheme,
            signed.signature.toString(),
            this.contractDeployerKeypair.getPublicKey().toString(),
            'WaitForLocalExecution',
        ]

        resp_json = await this.callRpc('sui_executeTransaction', params);
        for (let i = 0; i < resp_json.result.EffectsCert.effects.effects.created.length; i++) {
            const element = resp_json.result.EffectsCert.effects.effects.created[i];
            let packageId = element.reference.objectId;
            let key = Object.keys(this.suiConfig.contracts)[i];
            this.contracts[key] = packageId;
        }

        logger.info(`Deployed contracts: ${JSON.stringify(this.contracts)}`);
    }


    async prepareWorkerArguments(number) {
        let result = [];
        for (let i = 0; i < number; i++) {
            result[i] = {
                contracts: this.contracts
            };
        }

        return result;
    }


    async callRpc(method, params) {
        const data = {
            "jsonrpc": "2.0",
            "id": 1,
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
