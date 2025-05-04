// lib/solana-connection.js (v3.1.0 compatible - Hardcoded URLs Kept)
// - Updated default clientId version.
// - Added comments regarding endpoint filtering.

import { Connection } from '@solana/web3.js';

let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

// --- Define your specific endpoint details (Kept as requested) ---
// These are the ONLY endpoints that will be used by this class due to the filtering logic below.
// The endpoints array passed from index.js essentially acts as an allow-list for these URLs.
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
            // Note: Even if index.js provides URLs, this check runs first.
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs (from env var RPC_URLS).");
        }

        // ** KEPT: Hardcoded endpoint filtering logic **
        // This means only the specific Helius/QuickNode URLs defined above will be used,
        // regardless of other valid URLs potentially present in the 'endpoints' array passed from index.js.
        const allowedHardcodedUrls = [HELIUS_HTTPS_URL, HELIUS_HTTPS_URL_2, QUICKNODE_HTTPS_URL];
        const validEndpoints = endpoints.filter(ep => allowedHardcodedUrls.includes(ep));

        if (validEndpoints.length !== endpoints.length) {
            console.warn("[RLC] Constructor Warning: Input 'endpoints' array from RPC_URLS contained URLs not matching the hardcoded allowed URLs in lib/solana-connection.js. Only using matching hardcoded URLs.", { input: endpoints, allowed: allowedHardcodedUrls, used: validEndpoints });
        }
        if (validEndpoints.length === 0) {
            throw new Error("RateLimitedConnection: None of the RPC URLs from the environment variable matched the allowed hardcoded URLs in lib/solana-connection.js.");
        }
        // Use the filtered list from now on
        endpoints = validEndpoints;
        // ** END KEPT **

        const initialEndpoint = endpoints[0];
        // *** UPDATED: Default clientId fallback reflects new index.js version ***
        const clientId = options.clientId || `SolanaGamblesBot/3.1.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        // --- Determine initial WSS endpoint BEFORE calling super() ---
        const initialWssEndpoint = RateLimitedConnection._getWssEndpoint(initialEndpoint); // Use static helper
        if (!initialWssEndpoint) {
            console.warn(`[RLC] Could not determine initial WSS endpoint for ${initialEndpoint}. WebSocket features might fail initially.`);
        }

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

        // console.log(`[RLC] Calling super() for initial RPC endpoint: ${initialEndpoint}`); // Less verbose
        super(initialEndpoint, rpcOptions);
        // --- Now 'this' is available ---

        this.endpoints = [...endpoints]; // Store the filtered list
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
        // console.log("[RLC] RateLimitedConnection constructed successfully."); // Less verbose
    }

    // --- Helper to get WSS endpoint - NOW STATIC ---
    // ** KEPT: Hardcoded URL specific checks from original file **
    static _getWssEndpoint(httpsUrl) {
        if (httpsUrl === HELIUS_HTTPS_URL) return HELIUS_WSS_URL;
        if (httpsUrl === HELIUS_HTTPS_URL_2) return HELIUS_WSS_URL_2;
        if (httpsUrl === QUICKNODE_HTTPS_URL) return QUICKNODE_WSS_URL;

        // Fallback for unknown URLs (shouldn't happen with current filtering, but kept for safety)
        console.warn(`[RLC Helper Static] Unknown HTTPS URL provided to _getWssEndpoint: ${httpsUrl}. Attempting fallback conversion.`);
        try {
            return httpsUrl.replace(/^https:/, 'wss:');
        } catch (e) {
            console.error(`[RLC Helper Static] Error converting HTTPS to WSS for ${httpsUrl}: ${e.message}`);
            return undefined;
        }
    }
    // --- End Helper Modification ---

    // --- Remaining Methods (Unchanged from provided file) ---

    getCurrentEndpoint() {
        if (this.currentEndpointIndex < 0 || this.currentEndpointIndex >= this.endpoints.length) { this.currentEndpointIndex = 0; }
        if (this.endpoints.length === 0) { console.error("[RLC] No endpoints configured!"); return undefined; }
        return this.endpoints[this.currentEndpointIndex];
    }

    _rotateEndpoint() {
        if (this.endpoints.length <= 1) { return this.getCurrentEndpoint(); }
        const oldIndex = this.currentEndpointIndex;
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
        const newEndpoint = this.endpoints[this.currentEndpointIndex];
        const newWssEndpoint = RateLimitedConnection._getWssEndpoint(newEndpoint); // Use static helper
        this.stats.endpointRotations++;
        const getHostname = (url) => { try { return new URL(url).hostname; } catch { return url; } }
        console.log(`[RLC] Rotated RPC from index ${oldIndex} (${getHostname(this.endpoints[oldIndex])}) to ${this.currentEndpointIndex} (${getHostname(newEndpoint)})`);
        this._rpcEndpoint = newEndpoint; // Update underlying Connection properties
        this._rpcWsEndpoint = newWssEndpoint; // Update underlying Connection properties
        this.consecutiveRateLimits = 0; // Reset consecutive count on rotation
        return newEndpoint;
    }

    async _rpcRequest(method, args) {
        // Override _rpcRequest to use our queueing mechanism
        return this._enqueueRpcOperation(method, args);
    }

    _enqueueRpcOperation(method, args) {
        this.stats.totalRequestsEnqueued++;
        return new Promise((resolve, reject) => {
            const request = {
                method,
                args,
                resolve,
                reject,
                retries: 0,
                timestamp: Date.now(),
                originalEndpointIndex: this.currentEndpointIndex // Track which endpoint it started on
            };
            this.requestQueue.push(request);
            setImmediate(() => this._processQueue()); // Process queue asynchronously
        });
    }

    async _processQueue() {
        // Prevent multiple concurrent processing loops
        if (this._isProcessing) return;
        this._isProcessing = true;

        while (this.requestQueue.length > 0 && this.activeRequests < this.options.maxConcurrent) {
             // Check cooloff period due to rate limiting
             if (this.consecutiveRateLimits > 0) {
                 const timeSinceLast = Date.now() - this.lastRateLimitTime;
                 // Calculate exponential backoff for cooloff
                 const cooloff = Math.min(
                     this.options.rateLimitCooloff * (2 ** (this.consecutiveRateLimits - 1)),
                     this.options.retryMaxDelay // Cap cooloff at max retry delay
                 );

                 if (timeSinceLast < cooloff) {
                     // Still in cooloff, schedule next check
                     if (!this.cooloffTimeout) {
                         const delay = cooloff - timeSinceLast + 50; // Check again shortly after cooloff ends
                          // console.log(`[RLC] Rate limit cooloff active. Waiting ${delay.toFixed(0)}ms.`); // Verbose
                         this.cooloffTimeout = setTimeout(() => {
                             this.cooloffTimeout = null;
                             this._processQueue(); // Re-trigger queue processing
                         }, delay);
                         if (this.cooloffTimeout.unref) this.cooloffTimeout.unref(); // Allow exit if only timer remains
                     }
                      this._isProcessing = false; // Stop processing loop for now
                     return; // Exit while loop and function until cooloff ends
                 }
                 // Cooloff period ended, continue processing but don't reset consecutive count yet
             }


            this.activeRequests++;
            const request = this.requestQueue.shift(); // Get the next request
            const requestId = getNextRequestId(); // Assign unique ID for logging

            // Execute the request asynchronously without awaiting here, to allow processing next queue items
            this._executeRequest(request, requestId).then(() => {
                // Decrement active requests and potentially trigger next processing cycle
                this.activeRequests--;
                setImmediate(() => this._processQueue());
            }).catch((error) => {
                 // Should not happen if _executeRequest handles its own errors, but log just in case
                 console.error(`[RLC] Unexpected error after _executeRequest finished for Req ID ${requestId}:`, error);
                 this.activeRequests--;
                 setImmediate(() => this._processQueue());
            });
        }
         this._isProcessing = false; // Done processing for now
    }

     async _executeRequest(request, requestId) {
          const getHostname = (url) => { try { return new URL(url).hostname; } catch { return url; } }; // Local helper
          let attemptEndpoint = this.getCurrentEndpoint(); // Endpoint for this attempt

          try {
              if (!attemptEndpoint) { throw new Error("No valid RPC endpoint configured."); }
               // console.log(`[RLC] Sending Req ID ${requestId}: ${request.method} via ${getHostname(attemptEndpoint)} (Attempt ${request.retries + 1})`); // Verbose
              const result = await this._doRpcRequest(attemptEndpoint, request.method, request.args, requestId);
              request.resolve(result); // Resolve the original promise
              this.stats.totalRequestsSucceeded++;
              // Reset consecutive rate limits only on success AFTER a rate limit occurred
              if (this.consecutiveRateLimits > 0 && this.lastRateLimitTime < request.timestamp) {
                   // console.log(`[RLC] Successful request after rate limit period. Resetting consecutive count.`); // Verbose
                   this.consecutiveRateLimits = 0;
              }
          } catch (error) {
              const currentEndpointForError = attemptEndpoint || 'Unknown'; // Endpoint used for the failed attempt
              console.warn(`[RLC] RPC Attempt Failed: ${request.method} [Req ID ${requestId}] using ${getHostname(currentEndpointForError)}. Error: ${error.message}`);

              const isRateLimit = error.code === 429 || String(error.message).includes('429');
              const shouldRetry = request.retries < this.options.maxRetries;

              if (isRateLimit) {
                  this.stats.rateLimitEvents++;
                  this.lastRateLimitTime = Date.now();
                  this.consecutiveRateLimits++;
                  console.warn(`[RLC] Rate limit hit (Consecutive: ${this.consecutiveRateLimits}) on ${getHostname(currentEndpointForError)}. Rotating endpoint.`);
                  this._rotateEndpoint(); // Rotate endpoint immediately
                  // Re-queue the request at the front for immediate retry processing (respecting cooloff in _processQueue)
                  request.retries++; // Increment retry count even for rate limits to prevent infinite loops on persistent RL
                  this.requestQueue.unshift(request);

              } else if (shouldRetry) { // Non-rate-limit error, but still retryable attempts left
                  request.retries++;
                   console.log(`[RLC] Non-rate-limit error on ${requestId}. Attempt ${request.retries}/${this.options.maxRetries}. Rotating endpoint.`);
                  this._rotateEndpoint(); // Rotate endpoint
                  const delay = this._calculateRetryDelay(request.retries);
                  console.log(`[RLC] Scheduling retry for ${requestId} in ${delay.toFixed(0)}ms.`);
                  // Re-queue after a delay
                  setTimeout(() => {
                      this.requestQueue.unshift(request); // Add back to front after delay
                      this._processQueue(); // Ensure queue is processed after delay
                  }, delay);

              } else { // Max retries reached or non-retryable error
                  console.error(`[RLC] RPC Failed Permanently: ${request.method} [Req ID ${requestId}] after ${request.retries} retries. Last error: ${error.message}`);
                  this.stats.totalRequestsFailed++;
                  request.reject(error); // Reject the original promise
                  // Reset rate limit counter if failure wasn't a rate limit itself
                  if (!isRateLimit && this.consecutiveRateLimits > 0 && this.lastRateLimitTime < request.timestamp) {
                       this.consecutiveRateLimits = 0;
                  }
              }
          }
     }

    // _doRpcRequest performs the actual fetch call
    async _doRpcRequest(endpoint, method, params, requestId) {
        const payload = { jsonrpc: '2.0', id: requestId, method, params };
        let response;
        try {
            // Use AbortSignal for fetch timeout
            response = await fetch(endpoint, {
                method: 'POST',
                headers: this.baseHttpHeaders,
                body: JSON.stringify(payload),
                signal: AbortSignal.timeout(30000) // 30 second timeout
            });
        } catch (fetchError) {
            // Normalize fetch errors
            fetchError.code = (fetchError.name === 'TimeoutError' || fetchError.name === 'AbortError') ? 'FETCH_TIMEOUT' : 'FETCH_ERROR';
            throw fetchError;
        }

        if (!response.ok) {
            let errorText = `(Failed reading error body)`; // Default error text
            try {
                errorText = await response.text();
            } catch (e) { /* ignore */ }
            const httpError = new Error(`HTTP ${response.status} ${response.statusText}: ${errorText.substring(0, 200)}`);
            httpError.code = response.status; // Use HTTP status as error code
             if (response.status === 429) httpError.code = 429; // Ensure 429 code is set
            throw httpError;
        }

        let json;
        try {
            json = await response.json();
        } catch (parseError) {
            parseError.code = 'JSON_PARSE_ERROR'; // Add code for easier identification
            throw parseError;
        }

        if (json.error) {
            const rpcError = new Error(`RPC Error: ${json.error.message} (Code: ${json.error.code})`);
            rpcError.code = json.error.code;
            rpcError.data = json.error.data; // Include data if available
            throw rpcError;
        }

        // Check if 'result' property exists
        // Some methods might return successfully with no result (e.g., subscriptions sometimes?)
        // Adjust this check if specific methods require 'result' vs others don't.
        // For most standard RPC calls, 'result' is expected on success.
        if (!('result' in json)) {
             // If json.error didn't exist, but result is missing, it's odd.
             console.warn(`[RLC] RPC response missing 'result' field for method ${method}, but no error reported. Response:`, JSON.stringify(json).substring(0, 200));
             // Decide how to handle this - treat as error or return null/undefined?
             // For now, let's treat it as an invalid format error.
            const formatError = new Error(`Invalid RPC response: Missing 'result' field for method ${method}.`);
            formatError.code = "INVALID_RESPONSE_FORMAT";
            throw formatError;
        }
        return json.result;
    }

    _calculateRetryDelay(retries) {
        const base = this.options.retryBaseDelay;
        const jitterFactor = this.options.retryJitter; // e.g., 0.2
        // Calculate jitter: +/- (base * jitterFactor / 2)
        const jitter = base * jitterFactor * (Math.random() - 0.5);
        // Exponential backoff, capped at max delay
        const delay = Math.min(
            base * (2 ** (retries -1)) + jitter, // Start backoff from base for first retry
            this.options.retryMaxDelay
        );
        return Math.max(0, delay); // Ensure non-negative delay
    }

    getRequestStats() {
        const totalProcessed = this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed;
        return {
            status: {
                currentEndpointUrl: this.getCurrentEndpoint(),
                currentEndpointIndex: this.currentEndpointIndex,
                queueSize: this.requestQueue.length,
                activeRequests: this.activeRequests,
                consecutiveRateLimits: this.consecutiveRateLimits,
                lastRateLimitTimestamp: this.lastRateLimitTime > 0 ? new Date(this.lastRateLimitTime).toISOString() : null,
            },
            stats: {
                ...this.stats,
                successRate: totalProcessed > 0 ? (this.stats.totalRequestsSucceeded / totalProcessed).toFixed(4) : null
            }
        };
    }

    setMaxConcurrent(num) {
        if (typeof num === 'number' && num >= 1) {
            console.log(`[RLC] Setting max concurrent RPC requests to: ${num}`);
            this.options.maxConcurrent = Math.floor(num); // Ensure integer
            // Trigger processing immediately in case queue was waiting for concurrency slots
            setImmediate(() => this._processQueue());
        } else {
            console.warn(`[RLC] Invalid number provided for setMaxConcurrent: ${num}. Must be >= 1.`);
        }
    }
}

// Ensure _isProcessing state exists on the prototype or is initialized in constructor
RateLimitedConnection.prototype._isProcessing = false;


export default RateLimitedConnection;
