import * as functions from 'firebase-functions';
// Auth
import { _getDataPermissions, _connectBQ, _isAuthorized } from '../auth/index';
//BigQuery
import { BigQuery } from '@google-cloud/bigquery';
// Logger
import { _logger } from '../tools';

const DATASET = 'service';

/* ***************** */
/*        API        */
/* ***************** */


/**
 *  Function : arrearsDetails
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const arrearsDetails = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.arrears_details_pm\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Date BETWEEN '${startDate}' and '${endDate}'
                                ORDER BY Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('arrearsDetails', error);
        res.status(500).send(error);
    }
})


/**
 *  Function : arrearsSummary
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const arrearsSummary = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.arrears_summary_box\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('arrearsSummary', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : customers
 *  @param {string} customerId required
 *  
 */
export const customers = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Get request param
        const customerId = req.query.customerId || null;
        
        // Connect to BigQuery 
        const bqClient = await _connectBQ(req);

        // Build SQL query
        let query =   `SELECT Customer_ID 
                         FROM \`${bqClient.projectId}.${DATASET}.customer\` 
                         WHERE Customer_Status = 'Active' AND Version = 1`;
        
        if (customerId)
            query = query + ` AND LOWER(Customer_ID) like LOWER('%${customerId}%') `;

                
        // Execute query (add data permissions is not required here)
        const result = await _executeQuery(req, bqClient, query, true);

        res.send(result);
    }
    catch (error) {
        _logger('customers', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : disbursementsDetails
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const disbursementsDetails = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.disbursements_details\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Update_Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Update_Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('disbursementsDetails', error);
        res.status(500).send(error);
    }
})


/**
 *  Function : disbursementsSummary
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const disbursementsSummary = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.disbursements_summary_box\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Update_Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Update_Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('disbursementsSummary', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : eventsFeed
 * 
 *  @param {string} portfolioManagerId required
 *  @param {number} limit optional => default 100 
 *  @param {number} offset optional => default 1
 *  
 */
export const eventsFeed = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            // Get limit (nb records) or set default to 100;
            const limit = req.query.limit || 100;
            // Get offset (page) or set default to 1;
            const offset = req.query.offset || 1;

            // Connect to BigQuery 
            const bqClient = await _connectBQ(req);

            // Build SQL query
            const query =   `SELECT * 
                             FROM \`${bqClient.projectId}.${DATASET}.events_feed\`
                             WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                             ORDER BY Date_Time DESC
                             LIMIT ${limit} OFFSET ${offset}`;
            
                                
            // Execute query (add data permissions is not required here)
            const result = await _executeQuery(req, bqClient, query, true);

            res.send(result);
        }
    }
    catch (error) {
        _logger('eventsFeed', error);
        res.status(500).send(error);
    }
})


/**
 *  Function : hierarchy
 *  
 */
export const hierarchy = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToUsersManagement'])) {
            res.status(403).send();
        }
        else {
            // Connect to BigQuery 
            const bqClient = await _connectBQ(req);

            // Build SQL query
            const query =   `SELECT Country, Region, Department, Department_Name, Supervisor_ID, Portfolio_Manager_ID, Portfolio_Manager_Name
                             FROM \`${bqClient.projectId}.${DATASET}.hierarchy\`
                             WHERE Version = 1 AND Portfolio_Manager_Status = 'Active'`;
                                
            // Execute query (add data permissions is not required here)
            const result = await _executeQuery(req, bqClient, query, false);

            res.send(result);
        }
    }
    catch (error) {
        _logger('hierarchy', error);
        res.status(500).send(error);
    }
})


/**
 *  Function : maturityDetails
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const maturityDetails = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.maturing_details_pm\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Update_Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Update_Date DESC`;
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('maturityDetails', error);
        res.status(500).send(error);
    }
})


/**
 *  Function : maturitySummary
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const maturitySummary = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.maturing_summary_box\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Update_Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Update_Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('maturitySummary', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : outstandingDetails
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const outstandingDetails = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.outstanding_details_pm\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('outstandingDetails', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : outstandingSummary
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} startDate required format('YYYY-MM-DD')
 *  @param {string} endDate optional => equal startDate if null format('YYYY-MM-DD')
 *  
 */
export const outstandingSummary = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;
            const startDate = req.query.startDate;
            const endDate = req.query.endDate || startDate;

            if (!portfolioManagerId || !startDate)
                res.status(400).send('Parameter portfolioManagerId or startDate is missing');
            else {
                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                let query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.outstanding_summary_box\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                  AND Date BETWEEN '${startDate}' and '${endDate}' 
                                ORDER BY Date DESC`;
                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('outstandingSummary', error);
        res.status(500).send(error);
    }
})



/**
 *  Function : portfolioManagerInfo
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} date optional => default today
 */
export const portfolioManagerInfo = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;

            if (!portfolioManagerId)
                res.status(400).send('Parameter portfolioManagerId is missing');
            else {
                // Get date parameter or set today date if null
                const date = req.query.date || new Date().toISOString().slice(0, 10); // YYYY-MM-DD

                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                const query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.history_box_pm\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                AND Update_Date = '${date}'`;
                                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('portfolioManagerInfo', error);
        res.status(500).send(error);
    }
})




/**
 *  Function : portfolioManagerPerformanceSummary
 * 
 *  @param {string} portfolioManagerId required
 *  @param {string} date optional => default today
 */
export const portfolioManagerPerformanceSummary = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToOperationalPerformance'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const portfolioManagerId = req.query.portfolioManagerId;

            if (!portfolioManagerId)
                res.status(400).send('Parameter portfolioManagerId is missing');
            else {
                // Get date parameter or set today date if null
                const date = req.query.date || new Date().toISOString().slice(0, 10); // YYYY-MM-DD

                // Connect to BigQuery 
                const bqClient = await _connectBQ(req);

                // Build SQL query
                const query =   `SELECT * 
                                FROM \`${bqClient.projectId}.${DATASET}.summary_stats_pm\`
                                WHERE LOWER(Portfolio_Manager_ID) = LOWER('${portfolioManagerId}')
                                AND Update_Date = '${date}'`;
                                
                                    
                // Execute query (add data permissions is not required here)
                const result = await _executeQuery(req, bqClient, query, true);

                res.send(result);
            }
        }
    }
    catch (error) {
        _logger('portfolioManagerPerformanceSummary', error);
        res.status(500).send(error);
    }
})




/**
 *  Function : repaymentSchedule
 *  
 *  @param string customerId
 * 
 */
export const repaymentSchedule = functions.region("europe-west1").https.onRequest(async (req, res) => {

    try {
        // Check if the user has permission
        if (!await _isAuthorized(req, ['accessToCustomerDetails'])) {
            res.status(403).send();
        }
        else {
            // Get request param
            const customerId = req.query.customerId;
    
            // Connect to BigQuery 
            const bqClient = await _connectBQ(req);
    
            // Build SQL query
            const query =   `SELECT * 
                            FROM \`${bqClient.projectId}.${DATASET}.schedule_event\`
                            WHERE Customer_ID = '${customerId}'`;
    
            // Execute query (add data permissions is required here)
            const result = await _executeQuery(req, bqClient, query, true);
                            
            res.send(result);
        }
    }
    catch (error) {
        _logger('repaymentSchedule', error);
        res.status(500).send(error);
    }
})




/* ***************** */
/*  Shared functions */
/* ***************** */

export async function _executeQuery(req: functions.https.Request, bqClient: BigQuery, query: string, checkDataPermissions: boolean): Promise<any> {

    let _query: string;
    let index: number;
    
    // Get data permissions if required
    const permissions = checkDataPermissions ? await _getDataPermissions(req) : null;

    // If permissions is null or we execute the request as is
    if (!permissions) {
         _query = query
    }
    else {
        // Case WHERE
        index = query.toLowerCase().indexOf('where');
        if (index > -1)
            _query = `${query.slice(0, index + 5)} ${permissions} AND ${query.slice(index + 6)}`;
        else {
            // Case GROUP BY 
            index = query.toLowerCase().indexOf('group by');
            if (index > -1)
                _query = `${query.slice(0, index)} WHERE ${permissions} ${query.slice(index)}`;
            else {
                // Case ORDER BY 
                index = query.toLowerCase().indexOf('order by');
                if (index > -1)
                    _query = `${query.slice(0, index)} WHERE ${permissions} ${query.slice(index)}`;
                // Case only SELECT
                else {
                    _query = `${query} WHERE ${permissions}`;
                }
            }
        }
    }

    console.log(_query);

    const options = {
        query: _query,
        timeoutMs: 100000, // Time out after 100 seconds.
        useLegacySql: false, // Use standard SQL syntax for queries.
    };
        
    return bqClient.query(options);
}

