'use strict';

const SuiConnector = require('./sui-connector');

/**
 * Constructs an Sui adapter.
 * @param {number} workerIndex The zero-based index of the worker who wants to create an adapter instance. -1 for the manager process.
 * @return {Promise<ConnectorBase>} The initialized adapter instance.
 * @async
 */
async function connectorFactory(workerIndex) {
    return new SuiConnector(workerIndex, 'sui');
}

module.exports.ConnectorFactory = connectorFactory;
