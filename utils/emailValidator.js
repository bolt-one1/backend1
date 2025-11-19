const dns = require('dns').promises;
const emailValidator = require('email-validator');

// Enhanced email validation with live DNS checking
const validateEmailDomain = async (email) => {
  try {
    // First check basic email format
    if (!emailValidator.validate(email)) {
      return { valid: false, reason: 'Invalid email format' };
    }

    const domain = email.split('@')[1];
    
    // Check if domain has MX records (mail servers)
    try {
      const mxRecords = await dns.resolveMx(domain);
      if (!mxRecords || mxRecords.length === 0) {
        return { valid: false, reason: 'Domain has no mail servers' };
      }
    } catch (error) {
      return { valid: false, reason: 'Domain does not exist or has no mail servers' };
    }

    // Check if domain has A record (exists)
    try {
      await dns.resolve4(domain);
    } catch (error) {
      try {
        await dns.resolve6(domain);
      } catch (error2) {
        return { valid: false, reason: 'Domain does not exist' };
      }
    }

    return { valid: true, reason: 'Email is valid' };
  } catch (error) {
    return { valid: false, reason: 'Unable to validate email' };
  }
};

// List of common disposable email domains to block
const disposableEmailDomains = [
  '10minutemail.com',
  'tempmail.org',
  'guerrillamail.com',
  'mailinator.com',
  'throwaway.email',
  'temp-mail.org',
  'yopmail.com',
  'sharklasers.com',
  'grr.la',
  'guerrillamailblock.com',
  'pokemail.net',
  'spam4.me',
  'bccto.me',
  'chacuo.net',
  'dispostable.com',
  'fakeinbox.com'
];

const isDisposableEmail = (email) => {
  const domain = email.split('@')[1].toLowerCase();
  return disposableEmailDomains.includes(domain);
};

// Check if email exists (simplified check)
const checkEmailExists = async (email) => {
  try {
    const domain = email.split('@')[1];
    const mxRecords = await dns.resolveMx(domain);
    
    // This is a basic check - for production, you might want to use
    // a service like ZeroBounce, Hunter.io, or similar for actual email verification
    return mxRecords && mxRecords.length > 0;
  } catch (error) {
    return false;
  }
};

module.exports = {
  validateEmailDomain,
  isDisposableEmail,
  checkEmailExists
};