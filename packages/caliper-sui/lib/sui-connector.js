'use strict';

const {ConnectorBase, CaliperUtils, ConfigUtil, TxStatus} = require('@hyperledger/caliper-core');
const { JsonRpcProvider, Ed25519Keypair, RawSigner } = require('@mysten/sui.js');
const fs = require('fs/promises');

const logger = CaliperUtils.getLogger('ethereum-connector');

class SuiConnector extends ConnectorBase {
    constructor(workerIndex, bcType) {
        super(workerIndex, bcType);

        // Load config
        let configPath = CaliperUtils.resolvePath(
            ConfigUtil.get(ConfigUtil.keys.NetworkConfig)
        );
        this.suiConfig = require(configPath).sui;
        this.suiProvider = new JsonRpcProvider(this.suiConfig.url);
    }


    async init() {
        // Initialization
        if (this.suiConfig.contractDeployerAddressPrivateKey) {
            this.suiKeypair = Ed25519Keypair.fromSecretKey(this.suiConfig.contractDeployerAddressPrivateKey);
        } else {
            this.suiKeypair = Ed25519Keypair.generate();
        }
        this.suiSigner = new RawSigner(this.suiKeypair, this.suiProvider);
    }

    /**
     * Deploy smart contracts specified in the network configuration file.
     * @return {object} Promise execution for all the contract creations.
     */
    async installSmartContract() {
        logger.info('Creating contracts...');

        let promises = [];
        for (const key of Object.keys(this.suiConfig.contracts)) {
            const contract = this.suiConfig.contracts[key];
            const bytecode = await fs.readFile(contract, 'base64');
            promises.push(
                this.suiSigner.publish({
                    compiledModules: [bytecode.toString()],
                    gasBudget: 1000  // TODO: Estimate
                })
            );
        }

        return Promise.all(promises);
    }
}

module.exports = SuiConnector;
