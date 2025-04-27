// utils/performance.js
export const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;

        if (this.requests % 50 === 0) {
            const uptime = (Date.now() - this.startTime) / 1000;
            const errorRate = this.requests > 0 ? (this.errors / this.requests * 100).toFixed(1) : 0;
            console.log(`
📊 Performance Metrics:
    - Uptime: ${uptime.toFixed(0)}s
    - Total Requests Handled: ${this.requests}
    - Error Rate: ${errorRate}%
            `);
        }
    },
    // Add a method to get current stats maybe for health checks
    getStats() {
         return {
             requests: this.requests,
             errors: this.errors,
             uptime: (Date.now() - this.startTime) / 1000,
             errorRate: this.requests > 0 ? (this.errors / this.requests * 100).toFixed(1) : 0
         };
    }
};
