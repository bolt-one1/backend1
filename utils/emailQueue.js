const sendEmail = require('./sendEmail');

class EmailQueue {
  constructor() {
    this.queue = [];
    this.processing = false;
  }

  async add(emailData) {
    this.queue.push(emailData);

    if (!this.processing) {
      this.processQueue();
    }
  }

  async processQueue() {
    if (this.processing || this.queue.length === 0) {
      return;
    }

    this.processing = true;

    while (this.queue.length > 0) {
      const emailData = this.queue.shift();

      try {
        await sendEmail(emailData);
        console.log(`Email sent successfully to ${emailData.email}`);
      } catch (error) {
        console.error(`Failed to send email to ${emailData.email}:`, error.message);
      }
    }

    this.processing = false;
  }

  getQueueLength() {
    return this.queue.length;
  }
}

module.exports = new EmailQueue();
