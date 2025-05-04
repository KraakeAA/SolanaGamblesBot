// lib/solana-connection.js (v3.0.0 compatible - Hardcoded URLs Kept)
// - Updated default clientId ONLY.

import { Connection } from '@solana/web3.js';

let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

// --- Define your specific endpoint details (Kept as in original file) ---
const HELIUS_HTTPS_URL = 'https://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';
const HELIUS_WSS_URL = 'wss://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';

const HELIUS_HTTPS_URL_2 = 'https://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';
const HELIUS_WSS_URL_2 = 'wss://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';

const QUICKNODE_HTTPS_URL = 'https://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';
const QUICKNODE_WSS_URL = QUICKNODE_HTTPS_URL.replace(/^https:/, 'wss:');
// --- End endpoint definitions ---


class RateLimitedConnection extends Connection {
    constructor(endpoints, options = {}) {
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        // ** KEPT: Hardcoded endpoint filtering logic from original file **
        const validEndpoints = endpoints.filter(ep =>
            ep === HELIUS_HTTPS_URL || ep === QUICKNODE_HTTPS_URL || ep === HELIUS_HTTPS_URL_2
        );

        if (validEndpoints.length !== endpoints.length) {
             console.warn("[RLC] Constructor Warning: Input endpoints array contains URLs other than the expected Helius (2 keys) and QuickNode URLs. Only using valid ones.", { input: endpoints, valid: validEndpoints });
             if (validEndpoints.length === 0) {
                 throw new Error("RateLimitedConnection: No valid Helius or QuickNode endpoints provided in the endpoints array.");
             }
             endpoints = validEndpoints; // Use the filtered list
        }
        // ** END KEPT **

        const initialEndpoint = endpoints[0];
        // *** UPDATED: Default clientId fallback ***
        const clientId = options.clientId || `SolanaGamblesBot/3.0.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        // --- Determine initial WSS endpoint BEFORE calling super() ---
        const initialWssEndpoint = RateLimitedConnection._getWssEndpoint(initialEndpoint); // Use static helper
        if (!initialWssEndpoint) {
            console.warn(`[RLC] Could not determine initial WSS endpoint for ${initialEndpoint}. WebSocket features might fail initially.`);
        }
        // console.log(`[RLC] Determined initial WSS endpoint: ${initialWssEndpoint || 'None'}`); // Reduce noise

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': clientId, // Use determined clientId
                ...(options.httpHeaders || {}) // Allow override from index.js
            },
            wsEndpoint: initialWssEndpoint, // Pass explicit WSS endpoint
            disableRetryOnRateLimit: true, // We handle retries/rotation manually
            fetch: fetch // Use global fetch or polyfill if needed
        };

        console.log(`[RLC] Calling super() for initial RPC endpoint: ${initialEndpoint}`);
        super(initialEndpoint, rpcOptions);
        // --- Now 'this' is available ---

        this.endpoints = [...endpoints]; // Use the potentially filtered list
        this.currentEndpointIndex = 0;

        this.baseHttpHeaders = rpcOptions.httpHeaders; // Store constructed headers

        // Store options for retry/rate limit logic
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5,
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2,
        };

        // Initialize state
        this.requestQueue = [];
        this.activeRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;
        this.cooloffTimeout = null;

        // Initialize stats
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };
        // console.log("[RLC] RateLimitedConnection constructed successfully."); // Reduce noise
    }

    // --- Helper to get WSS endpoint - NOW STATIC ---
    // ** KEPT: Hardcoded URL specific checks from original file **
    static _getWssEndpoint(httpsUrl) {
        if (httpsUrl === HELIUS_HTTPS_URL) {
            return HELIUS_WSS_URL;
        } else if (httpsUrl === HELIUS_HTTPS_URL_2) {
            return HELIUS_WSS_URL_2;
        } else if (httpsUrl === QUICKNODE_HTTPS_URL) {
            return QUICKNODE_WSS_URL;
        } else {
            console.warn(`[RLC Helper Static] Unknown HTTPS URL provided: ${httpsUrl}. Cannot determine specific WSS URL. Attempting fallback.`);
            try {
                // Fallback attempt
                return httpsUrl.replace(/^https:/, 'wss:');
            } catch (e) {
                console.error(`[RLC Helper Static] Error converting HTTPS to WSS for ${httpsUrl}: ${e.message}`);
                return undefined;
            }
        }
    }
    // --- End Helper Modification ---

    getCurrentEndpoint() {
        // (Unchanged from previous versions)
        if (this.currentEndpointIndex < 0 || this.currentEndpointIndex >= this.endpoints.length) { this.currentEndpointIndex = 0; }
        if (this.endpoints.length === 0) { console.error("[RLC] No endpoints configured!"); return undefined; }
        return this.endpoints[this.currentEndpointIndex];
    }

     _rotateEndpoint() {
         // (Unchanged from previous versions)
         if (this.endpoints.length <= 1) { return this.getCurrentEndpoint(); }
         const oldIndex = this.currentEndpointIndex;
         this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
         const newEndpoint = this.endpoints[this.currentEndpointIndex];
         const newWssEndpoint = RateLimitedConnection._getWssEndpoint(newEndpoint);
         this.stats.endpointRotations++;
         const getHostname = (url) => { try { return new URL(url).hostname; } catch { return url; } }
         console.log(`[RLC] Rotated RPC from index ${oldIndex} (${getHostname(this.endpoints[oldIndex])}) to ${this.currentEndpointIndex} (${getHostname(newEndpoint)})`);
         this._rpcEndpoint = newEndpoint;
         this._rpcWsEndpoint = newWssEndpoint;
         this.consecutiveRateLimits = 0;
         return newEndpoint;
     }

    // Override _rpcRequest (Unchanged)
    async _rpcRequest(method, args) {
        return this._enqueueRpcOperation(method, args);
    }

    // _enqueueRpcOperation (Unchanged)
    _enqueueRpcOperation(method, args) { /* ... same logic ... */ this.stats.totalRequestsEnqueued++; return new Promise((resolve, reject) => { const request = { method, args, resolve, reject, retries: 0, timestamp: Date.now(), originalEndpointIndex: this.currentEndpointIndex }; this.requestQueue.push(request); setImmediate(() => this._processQueue()); }); }

    // _processQueue (Unchanged - includes queue logic, rate limit handling, retries, rotation calls)
    async _processQueue() { /* ... same logic ... */ if (this.activeRequests >= this.options.maxConcurrent) return; if (this.requestQueue.length === 0) return; if (this.consecutiveRateLimits > 0) { const timeSinceLast = Date.now() - this.lastRateLimitTime; const cooloff = Math.min( this.options.rateLimitCooloff * (2 ** (this.consecutiveRateLimits - 1)), this.options.retryMaxDelay ); if (timeSinceLast < cooloff) { if (!this.cooloffTimeout) { const delay = cooloff - timeSinceLast + 50; this.cooloffTimeout = setTimeout(() => { this.cooloffTimeout = null; this._processQueue(); }, delay); if (this.cooloffTimeout.unref) this.cooloffTimeout.unref(); } return; } } this.activeRequests++; const request = this.requestQueue.shift(); const requestId = getNextRequestId(); try { const currentEndpoint = this.getCurrentEndpoint(); if (!currentEndpoint) { throw new Error("No valid RPC endpoint configured."); } const result = await this._doRpcRequest(currentEndpoint, request.method, request.args, requestId); request.resolve(result); this.stats.totalRequestsSucceeded++; if (this.consecutiveRateLimits > 0) { this.consecutiveRateLimits = 0; } } catch (error) { const currentEndpointForError = this.endpoints[request.originalEndpointIndex] || this.getCurrentEndpoint() || 'Unknown'; console.warn(`[RLC] RPC Attempt Failed: ${request.method} [Req ID ${requestId}] using ${getHostname(currentEndpointForError)}. Error: ${error.message}`); if (error.code === 429 || (error.message && error.message.includes('429'))) { this.stats.rateLimitEvents++; this.lastRateLimitTime = Date.now(); this.consecutiveRateLimits++; console.warn(`[RLC] Rate limit (Consecutive: ${this.consecutiveRateLimits}) on ${getHostname(currentEndpointForError)}. Rotating.`); this._rotateEndpoint(); this.requestQueue.unshift(request); } else { const shouldRetry = request.retries < this.options.maxRetries; if (shouldRetry) { request.retries++; console.log(`[RLC] Retrying ${requestId}. Attempt ${request.retries}/${this.options.maxRetries}. Rotating.`); this._rotateEndpoint(); const delay = this._calculateRetryDelay(request.retries); console.log(`[RLC] Retrying ${requestId} in ${delay.toFixed(0)}ms.`); setTimeout(() => { this.requestQueue.unshift(request); this._processQueue(); }, delay); } else { console.error(`[RLC] RPC Failed Permanently: ${request.method} [Req ID ${requestId}] after ${request.retries} retries. Error: ${error.message}`); this.stats.totalRequestsFailed++; request.reject(error); if (this.consecutiveRateLimits > 0) { this.consecutiveRateLimits = 0; } } } } finally { this.activeRequests--; setImmediate(() => this._processQueue()); } }

    // _doRpcRequest (Unchanged - performs fetch)
    async _doRpcRequest(endpoint, method, params, requestId) { /* ... same logic ... */ const payload = { jsonrpc: '2.0', id: requestId, method, params }; let response; try { response = await fetch(endpoint, { method: 'POST', headers: this.baseHttpHeaders, body: JSON.stringify(payload), signal: AbortSignal.timeout(30000) }); } catch (fetchError) { fetchError.code = (fetchError.name === 'TimeoutError' || fetchError.name === 'AbortError') ? 'FETCH_TIMEOUT' : 'FETCH_ERROR'; throw fetchError; } if (!response.ok) { let errorText; try { errorText = await response.text(); } catch (e) { errorText = `(Failed reading error body: ${e.message})`; } const httpError = new Error(`HTTP ${response.status} ${response.statusText}: ${errorText.substring(0, 200)}`); httpError.code = response.status; if (response.status === 429) httpError.code = 429; throw httpError; } let json; try { json = await response.json(); } catch (parseError) { parseError.code = 'JSON_PARSE_ERROR'; throw parseError; } if (json.error) { const rpcError = new Error(`RPC Error: ${json.error.message} (Code: ${json.error.code})`); rpcError.code = json.error.code; rpcError.data = json.error.data; throw rpcError; } if (!('result' in json)) { const formatError = new Error("Invalid RPC response: Missing 'result'."); formatError.code = "INVALID_RESPONSE_FORMAT"; throw formatError; } return json.result; }

    // _calculateRetryDelay (Unchanged)
    _calculateRetryDelay(retries) { /* ... same logic ... */ const base = this.options.retryBaseDelay; const jitterFactor = this.options.retryJitter; const jitter = base * jitterFactor * (Math.random() - 0.5); const delay = Math.min( base * (2 ** retries) + jitter, this.options.retryMaxDelay ); return Math.max(0, delay); }

    // getRequestStats (Unchanged)
    getRequestStats() { /* ... same logic ... */ const totalProcessed = this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed; return { status: { currentEndpointUrl: this.getCurrentEndpoint(), currentEndpointIndex: this.currentEndpointIndex, queueSize: this.requestQueue.length, activeRequests: this.activeRequests, consecutiveRateLimits: this.consecutiveRateLimits, lastRateLimitTimestamp: this.lastRateLimitTime > 0 ? this.lastRateLimitTime : null, }, stats: { ...this.stats, successRate: totalProcessed > 0 ? (this.stats.totalRequestsSucceeded / totalProcessed) : null } }; }

    // setMaxConcurrent (Unchanged)
     setMaxConcurrent(num) { /* ... same logic ... */ if (typeof num === 'number' && num >= 1) { console.log(`[RLC] Setting max concurrent to: ${num}`); this.options.maxConcurrent = num; setImmediate(() => this._processQueue()); } else { console.warn(`[RLC] Invalid number for setMaxConcurrent: ${num}`); } }
}

export default RateLimitedConnection;
