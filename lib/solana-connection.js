// lib/solana-connection.js (Corrected Rotation + Debug Logs - Final Version)

import { Connection } from '@solana/web3.js';
// import { RpcError } from '@solana/rpc-errors'; // REMOVED - Package not found/needed

// Helper to generate unique IDs for RPC requests
let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

class RateLimitedConnection extends Connection {
    /**
     * Creates a rate-limited connection that rotates through multiple RPC endpoints.
     * @param {string[]} endpoints - An array of RPC endpoint URLs.
     * @param {object} options - Configuration options.
     * @param {string} [options.commitment='confirmed'] - Default commitment level.
     * @param {object} [options.httpHeaders={}] - Additional HTTP headers (applied to our fetch calls).
     * @param {string} [options.wsEndpoint] - Optional WebSocket endpoint (WS rotation needs careful handling).
     * @param {string} [options.clientId='multi-rpc-connection'] - Client identifier for headers.
     * @param {number} [options.maxConcurrent=5] - Max parallel requests.
     * @param {number} [options.retryBaseDelay=600] - Initial delay for retries (ms).
     * @param {number} [options.maxRetries=5] - Max attempts per request (per endpoint cycle).
     * @param {number} [options.rateLimitCooloff=5000] - Min pause duration after hitting 429 (ms).
     * @param {number} [options.retryMaxDelay=30000] - Max delay for exponential backoff (ms).
     * @param {number} [options.retryJitter=0.2] - Percentage jitter for retry delay (0.2 = +/- 20%).
     */
    constructor(endpoints, options = {}) {
        // --- 1. Validate Inputs ---
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        // --- 2. Prepare Arguments for super() ---
        const initialEndpoint = endpoints[0];
        const clientId = options.clientId || `SolanaGamblesBot/2.1-multi-rpc (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
             httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined,
            disableRetryOnRateLimit: true
        };

        // --- 3. Call super() FIRST ---
        console.log(`[RLC] Calling super() with initial endpoint: ${initialEndpoint}`);
        super(initialEndpoint, rpcOptions);
        console.log(`[RLC] super() call finished.`);

        // --- 4. Assign 'this' properties AFTER super() ---
        this.endpoints = [...endpoints];
        this.currentEndpointIndex = 0;

        // --- ADD THIS LOG ---
        console.log('[RLC Constructor] Endpoint count:', this.endpoints.length, 'Endpoints:', this.endpoints);
        // --- END ADD ---

        // Store base headers for our fetch calls
        this.baseHttpHeaders = {
             'Content-Type': 'application/json',
             'solana-client': clientId,
             ...(options.httpHeaders || {})
        };

        // --- Configuration ---
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5,
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2,
            wsEndpoint: options.wsEndpoint || undefined,
            commitment: options.commitment || 'confirmed'
        };

        // --- State ---
        this.requestQueue = [];
        this.activeRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;
        this.cooloffTimeout = null;

        // --- Statistics ---
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };

        console.log("[RLC] RateLimitedConnection (Multi-RPC - _rpcRequest Override) initialized with options:", this.options);
        console.log(`[RLC] Endpoints configured: ${this.endpoints.join(', ')}`);
    }

    // --- Multi-RPC Methods ---

    getCurrentEndpoint() {
        // Ensure index is valid, fallback to 0 if somehow out of bounds
        if (this.currentEndpointIndex < 0 || this.currentEndpointIndex >= this.endpoints.length) {
            console.warn(`[RLC] Invalid currentEndpointIndex (${this.currentEndpointIndex}), resetting to 0.`);
            this.currentEndpointIndex = 0;
        }
        return this.endpoints[this.currentEndpointIndex];
    }

    getEndpointCount() {
        return this.endpoints.length;
    }

    // MODIFIED: Removed the problematic this.rpcEndpoint assignment
    _rotateEndpoint() {
        // --- ADD THIS LOG ---
        console.log(`[RLC _rotateEndpoint ENTRY] Attempting to rotate. Current index: ${this.currentEndpointIndex}`);
        // --- END ADD ---

        if (this.endpoints.length <= 1) {
            return this.getCurrentEndpoint();
        }

        const oldIndex = this.currentEndpointIndex;
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
        const newEndpoint = this.endpoints[this.currentEndpointIndex]; // Get new endpoint URL for logging

        // Handle WebSocket rotation if necessary (remains complex)
        if (!this.options.wsEndpoint && this._wsEndpoint) {
             try {
                const url = new URL(newEndpoint);
                url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
                const newWsEndpoint = url.toString();
                if (this._wsEndpoint !== newWsEndpoint) {
                     console.log(`[RLC] Updating internal WebSocket endpoint reference to: ${newWsEndpoint} (Manual reconnect might be needed)`);
                     this._wsEndpoint = newWsEndpoint;
                     // TODO: Consider closing/reopening WS connection if active subscriptions exist
                }
            } catch(e) {
                console.error(`[RLC] Failed to derive WebSocket endpoint from ${newEndpoint}`, e);
            }
        }

        this.stats.endpointRotations++;
        // Keep original log too
        console.log(`[RLC] Rotated RPC endpoint index from ${oldIndex} to ${this.currentEndpointIndex}. New active URL: ${newEndpoint}. Rotations: ${this.stats.endpointRotations}`);

        this.resetRateLimitCounters();
        return newEndpoint; // Return the URL string for convenience, though not strictly needed internally now
    }

    // --- Overridden Methods ---

    /**
     * Intercepts RPC requests and sends them through the rate-limited queue.
     * @param {string} method The RPC method name.
     * @param {unknown[]} args The RPC method arguments.
     * @returns {Promise<unknown>} A promise that resolves to the RPC result.
     * @override
     * @private
     */
    async _rpcRequest(method, args) {
         // All requests are funneled through our queuing mechanism
         return this._enqueueRpcOperation(method, args);
    }

    /**
     * Enqueues an RPC operation.
     * @param {string} method The RPC method name.
     * @param {unknown[]} args The RPC method arguments.
     * @returns {Promise<unknown>} A promise resolving with the result or rejecting with an error.
     * @private
     */
    async _enqueueRpcOperation(method, args) {
        this.stats.totalRequestsEnqueued++;
        return new Promise((resolve, reject) => {
            const request = {
                method,
                args,
                resolve,
                reject,
                retries: 0,
                timestamp: Date.now(),
                originalEndpointIndex: this.currentEndpointIndex
            };
            this.requestQueue.push(request);
            setImmediate(() => this._processQueue()); // Ensure queue processing is scheduled
        });
    }

     // Alias for potential external compatibility if needed, though unlikely
     _enqueueRequest = this._enqueueRpcOperation;

    /**
     * Processes the next request in the queue if conditions allow.
     * @private
     */
    async _processQueue() {
         // Check conditions preventing processing
         if (this.activeRequests >= this.options.maxConcurrent) return;
         if (this.requestQueue.length === 0) return;

         // Check rate limit cooloff
         if (this.consecutiveRateLimits > 0) {
            const timeSinceLastRateLimit = Date.now() - this.lastRateLimitTime;
            // Exponential cooloff based on consecutive fails
            const requiredCooloff = this.options.rateLimitCooloff * Math.pow(2, this.consecutiveRateLimits -1);
            // Cap cooloff duration
            const cappedCooloff = Math.min(requiredCooloff, this.options.retryMaxDelay);
            if (timeSinceLastRateLimit < cappedCooloff) {
                if (!this.cooloffTimeout) { // Avoid setting multiple timeouts
                    const remainingCooloff = cappedCooloff - timeSinceLastRateLimit;
                    // console.log(`[RLC _processQueue] Cooling off for ${remainingCooloff.toFixed(0)}ms...`); // Reduce noise
                    this.cooloffTimeout = setTimeout(() => {
                        this.cooloffTimeout = null;
                        this._processQueue();
                    }, remainingCooloff + 50); // Add buffer
                }
                return; // Still cooling off
            }
            // console.log(`[RLC _processQueue] Cooloff finished (${timeSinceLastRateLimit}ms >= ${cappedCooloff}ms).`); // Reduce noise
         }

        // Dequeue and process
        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            // --- Execute the RPC call using our internal method ---
            // console.log(`[RLC _processQueue] Processing: ${request.method}`); // Verbose
            const result = await this._doRpcRequest(request.method, request.args);

            // --- Success ---
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            // Reset consecutive rate limits on success
            if (this.consecutiveRateLimits > 0) {
                // console.log(`[RLC _processQueue] Resetting consecutive rate limits after success.`); // Reduce noise
                this.consecutiveRateLimits = 0;
            }
        } catch (error) {
            // --- Error Handling & Retry Logic ---
            // --- ADD THIS LOG ---
            console.log(`[RLC _processQueue CATCH] Error for ${request.method}. Retries done: ${request.retries}, Endpoint Index: ${this.currentEndpointIndex}`);
            // --- END ADD ---

            const isRpcSpecific = this._isRpcSpecificError(error);
            const shouldRetry = this._shouldRetry(error, request.retries, isRpcSpecific);

            // --- ADD THIS LOG ---
            console.log(`[RLC _processQueue CATCH] isRpcSpecific: ${isRpcSpecific}, shouldRetry: ${shouldRetry}, endpoints.length: ${this.endpoints.length}`);
            // --- END ADD ---

            if (shouldRetry) {
                // --- ADD THIS LOG ---
                console.log(`[RLC _processQueue CATCH] Entering shouldRetry block...`);
                // --- END ADD ---

                request.retries++;
                let rotated = false;
                if (isRpcSpecific && this.endpoints.length > 1) {
                     // --- ADD THIS LOG ---
                     console.log(`[RLC _processQueue CATCH] Conditions for rotation met (isRpcSpecific && length > 1). Current index: ${this.currentEndpointIndex}`);
                     // --- END ADD ---

                     const nextIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
                     // Rotate unless it's immediately back to original (and more than 2 endpoints total, and haven't already cycled through)
                     if (nextIndex !== request.originalEndpointIndex || this.endpoints.length <= 2 || request.retries >= this.endpoints.length) {
                         // --- ADD THIS LOG ---
                         console.log(`[RLC _processQueue CATCH] >>>>>>> Calling _rotateEndpoint() <<<<<<<`);
                         // --- END ADD ---
                         this._rotateEndpoint();
                         rotated = true;
                     } else {
                         console.log(`[RLC _processQueue CATCH] Skipping rotation to avoid immediate return to original endpoint index ${request.originalEndpointIndex}.`);
                     }
                }

                const delay = this._calculateRetryDelay(request.retries);
                console.log(`[RLC] Retrying ${request.method} after ${delay.toFixed(0)}ms delay (Retry ${request.retries}/${this.options.maxRetries}).${rotated ? ` Rotated flag set true.` : ''}`);

                try {
                    await new Promise(resolve => setTimeout(resolve, delay));
                    this._requeueRequest(request); // Requeue at the front
                } catch (requeueError) {
                    console.error(`[RLC] Error during requeue wait/logic for ${request.method}:`, requeueError);
                    this.stats.totalRequestsFailed++;
                    request.reject(error); // Reject with the original error if requeue fails
                }
            } else {
                 // --- ADD THIS LOG ---
                 console.log(`[RLC _processQueue CATCH] Not retrying. Final failure.`);
                 // --- END ADD ---
                console.error(`[RLC] Request ${request.method} failed permanently after ${request.retries} retries. Last endpoint used: ${this.getCurrentEndpoint()}. Error: ${error.message}`);
                this.stats.totalRequestsFailed++;
                request.reject(error); // Reject the promise with the final error
            }
        } finally {
            this.activeRequests--;
            // Process next item if possible
            setImmediate(() => this._processQueue());
        }
    }

    /**
     * Performs the actual HTTP fetch for the RPC request.
     * @param {string} method The RPC method name.
     * @param {unknown[]} params The RPC method arguments.
     * @returns {Promise<unknown>} A promise that resolves to the RPC result.
     * @private
     */
    async _doRpcRequest(method, params) {
        const currentEndpoint = this.getCurrentEndpoint();
        const requestId = getNextRequestId();
        const payload = {
            jsonrpc: '2.0',
            id: requestId,
            method: method,
            params: params,
        };

        const jitter = Math.random() * 50;
        await new Promise(r => setTimeout(r, jitter));

        // console.log(`[RLC _doRpcRequest] Sending RPC ${method} (ID: ${requestId}) to ${currentEndpoint}`); // Verbose

        let response;
        try {
            // Use fetch API (requires Node 18+)
            response = await fetch(currentEndpoint, {
                method: 'POST',
                headers: this.baseHttpHeaders, // Use stored headers
                body: JSON.stringify(payload),
                signal: AbortSignal.timeout(30000) // 30 second timeout
            });
        } catch (fetchError) {
             console.error(`[RLC _doRpcRequest] Fetch error for ${method} on ${currentEndpoint}:`, fetchError.message);
             fetchError.code = fetchError.name === 'TimeoutError' ? 504 : 503; // Assign approximate HTTP codes
             throw fetchError;
        }

        // Handle non-200 HTTP status codes
        if (!response.ok) {
             const status = response.status;
             let errorText = response.statusText;
             try { errorText = await response.text(); } catch (_) { /* ignore */ }
             console.error(`[RLC _doRpcRequest] HTTP Error ${status} for ${method} on ${currentEndpoint}: ${errorText.slice(0, 500)}`);
              const httpError = new Error(`HTTP error ${status}: ${errorText.slice(0,100)}`);
              httpError.code = status; // Use HTTP status as error code
              httpError.response = { status }; // Mimic structure for _shouldRetry
              throw httpError;
        }

        // Parse JSON response
        let responseJson;
        try {
            responseJson = await response.json();
        } catch (parseError) {
            console.error(`[RLC _doRpcRequest] JSON parse error for ${method} response from ${currentEndpoint}:`, parseError.message);
             const jsonError = new Error(`Failed to parse JSON response: ${parseError.message}`);
             jsonError.code = 500; // Treat as server error
             throw jsonError;
        }

        // Check for JSON-RPC level errors
        if (responseJson.error) {
            // console.warn(`[RLC _doRpcRequest] RPC Error for ${method} (ID: ${requestId}) on ${currentEndpoint}:`, responseJson.error); // Reduce noise if logged in _processQueue
             const rpcErrorData = responseJson.error;
             // Use generic Error as fallback
             const errorToThrow = new Error(`RPC error ${rpcErrorData.code || 'N/A'}: ${rpcErrorData.message}`);
             errorToThrow.code = rpcErrorData.code;
             errorToThrow.data = rpcErrorData.data;
             if (!errorToThrow.message) errorToThrow.message = `RPC error code ${rpcErrorData.code || 'N/A'}`;
             if (!errorToThrow.code) errorToThrow.code = rpcErrorData.code;
             throw errorToThrow;
        }

        // Check if the response ID matches the request ID
        if (responseJson.id !== requestId) {
             console.warn(`[RLC _doRpcRequest] RPC Response ID mismatch! Expected ${requestId}, got ${responseJson.id}. Endpoint: ${currentEndpoint}`);
             const idMismatchError = new Error(`RPC Response ID mismatch. Expected ${requestId}, got ${responseJson.id}`);
             idMismatchError.code = 500; // Treat as server error
             throw idMismatchError;
        }

        // Return the successful result
        return responseJson.result;
    }

    // --- Helper methods for retries and errors ---

    _calculateRetryDelay(retryCount) {
        const baseDelay = this.options.retryBaseDelay;
        const maxDelay = this.options.retryMaxDelay;
        const jitterRatio = this.options.retryJitter;
        // Exponential backoff based on retries AND consecutive rate limits
        const retryFactor = Math.pow(2, retryCount - 1);
        const rateLimitFactor = Math.pow(2, this.consecutiveRateLimits);
        const exponentialDelay = baseDelay * retryFactor * rateLimitFactor;

        const cappedDelay = Math.min(exponentialDelay, maxDelay);
        const jitterMagnitude = cappedDelay * jitterRatio;
        const jitter = (Math.random() * 2 - 1) * jitterMagnitude;
        const finalDelay = Math.max(50, cappedDelay + jitter); // Minimum 50ms delay

         // If we were rate limited recently, enforce minimum cooloff
          if (this.consecutiveRateLimits > 0) {
               const requiredCooloff = this.options.rateLimitCooloff * Math.pow(2, this.consecutiveRateLimits -1);
               const cappedCooloff = Math.min(requiredCooloff, maxDelay);
              return Math.max(finalDelay, cappedCooloff);
          }

        return finalDelay;
    }

    _shouldRetry(error, retryCount, isRpcSpecific) {
        if (retryCount >= this.options.maxRetries) {
            return false; // Max retries exceeded
        }
        const errorMessage = error?.message?.toLowerCase() || '';
         // Use 'code' property directly if it exists (like from HTTP errors or RPC errors), otherwise check status
        const errorCode = error?.code;
        const errorStatus = error?.response?.status || error?.statusCode;

         // Check for Rate Limit (429)
         if (errorCode === 429 || String(errorCode) === '429' || errorStatus === 429 || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
              // Update rate limit stats only if not within cooloff period of the last 429 event
              const effectiveCooloff = this.options.rateLimitCooloff * Math.pow(2, Math.max(0, this.consecutiveRateLimits -1));
              const cappedCooloff = Math.min(effectiveCooloff, this.options.retryMaxDelay);
              if (this.lastRateLimitTime === 0 || (Date.now() - this.lastRateLimitTime) >= cappedCooloff) {
                 this.consecutiveRateLimits++;
                 this.stats.rateLimitEvents++;
                 console.warn(`[RLC] Rate limit detected! Consecutive: ${this.consecutiveRateLimits}. Cooling off.`);
              } else {
                 // console.log(`[RLC] Rate limit detected within cooloff period, not incrementing consecutive count.`); // Reduce noise
              }
              this.lastRateLimitTime = Date.now(); // Always update timestamp
             return true;
         }

         // Check for common retryable network/server errors (Codes or Status)
         const retryableStatusCodes = [500, 502, 503, 504]; // Server-side issues or timeouts
         if (retryableStatusCodes.includes(Number(errorCode)) || retryableStatusCodes.includes(Number(errorStatus))) {
            return true;
         }

         // Check common retryable network error messages/codes
          const retryableMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'connection refused'];
          const retryableErrorCodes = ['etimedout', 'econnreset', 'enetunreach', 'eai_again', 'econnaborted', 'econnrefused']; // Node.js specific
          if (retryableMessages.some(msg => errorMessage.includes(msg)) || (errorCode && retryableErrorCodes.includes(String(errorCode).toLowerCase()))) {
              return true;
          }

         // Retry simulation errors only if deemed RPC specific (potential temporary load)
          if (isRpcSpecific && (errorMessage.includes('transaction simulation failed') || errorMessage.includes('failed to simulate'))) {
              return true;
          }

         // If none of the above, assume not retryable
         return false;
     }

      _isRpcSpecificError(error) {
          const errorMessage = error?.message?.toLowerCase() || '';
          const errorCode = error?.code;
          const errorStatus = error?.response?.status || error?.statusCode;

          // Rate limits (429)
          if (errorCode === 429 || String(errorCode) === '429' || errorStatus === 429 || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
              return true;
          }
          // Server errors (5xx)
          if ([500, 502, 503, 504].includes(Number(errorCode)) || [500, 502, 503, 504].includes(Number(errorStatus))) {
               return true;
          }
           // Connection timeouts / refusals / resets are endpoint specific
            const networkErrorMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'econnrefused', 'failed to fetch', 'socket hang up', 'connection refused'];
            const networkErrorCodes = ['etimedout', 'econnreset', 'econnaborted', 'econnrefused'];
            if (networkErrorMessages.some(msg => errorMessage.includes(msg)) || (errorCode && networkErrorCodes.includes(String(errorCode).toLowerCase())) ) {
                return true;
            }
          // Simulation failures can sometimes be endpoint specific load issues
          if (errorMessage.includes('transaction simulation failed') || errorMessage.includes('failed to simulate')) {
               return true;
          }
           // Health check failures
            if (errorMessage.includes('health check failed')) {
                return true;
            }

          return false; // Assume other errors (like bad request 400, blockhash not found 410) are not endpoint-specific
        }

     _requeueRequest(request) {
        // Requeue at the front for immediate reprocessing after delay
        this.requestQueue.unshift(request);
    }

    // --- Utility Methods ---

     getRequestStats() {
        const completedRequests = this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed;
        const successRate = completedRequests > 0
            ? ((this.stats.totalRequestsSucceeded) / completedRequests * 100).toFixed(2)
            : 'N/A';
        return {
            options: { maxConcurrent: this.options.maxConcurrent, maxRetries: this.options.maxRetries, rateLimitCooloff: this.options.rateLimitCooloff, retryBaseDelay: this.options.retryBaseDelay, endpointsCount: this.endpoints.length },
            status: { queueSize: this.requestQueue.length, activeRequests: this.activeRequests, currentEndpoint: this.getCurrentEndpoint(), currentEndpointIndex: this.currentEndpointIndex, consecutiveRateLimits: this.consecutiveRateLimits, lastRateLimit: this.lastRateLimitTime > 0 ? `${Math.floor((Date.now() - this.lastRateLimitTime) / 1000)}s ago` : 'never', isCoolingOff: !!this.cooloffTimeout },
            stats: { totalEnqueued: this.stats.totalRequestsEnqueued, totalSucceeded: this.stats.totalRequestsSucceeded, totalFailed: this.stats.totalRequestsFailed, rateLimitEvents: this.stats.rateLimitEvents, endpointRotations: this.stats.endpointRotations, successRate: `${successRate}%` }
        };
    }

     resetRateLimitCounters() {
        this.consecutiveRateLimits = 0;
        // Don't reset lastRateLimitTime here
        if (this.cooloffTimeout) {
            clearTimeout(this.cooloffTimeout);
            this.cooloffTimeout = null;
        }
    }

} // End class RateLimitedConnection

export default RateLimitedConnection;
