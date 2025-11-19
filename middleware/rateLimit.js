const rateLimit = require('express-rate-limit');

const createAuthLimiter = () => {
  return rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 5,
    message: {
      success: false,
      message: 'Too many authentication attempts. Please try again after 15 minutes.'
    },
    standardHeaders: true,
    legacyHeaders: false,
    skipSuccessfulRequests: false
  });
};

const createPasswordResetLimiter = () => {
  return rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 3,
    message: {
      success: false,
      message: 'Too many password reset attempts. Please try again after 1 hour.'
    },
    standardHeaders: true,
    legacyHeaders: false,
    skipSuccessfulRequests: false
  });
};

const createVerificationLimiter = () => {
  return rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 3,
    message: {
      success: false,
      message: 'Too many verification email requests. Please try again after 1 hour.'
    },
    standardHeaders: true,
    legacyHeaders: false,
    skipSuccessfulRequests: false
  });
};

const createRegistrationLimiter = () => {
  return rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 3,
    message: {
      success: false,
      message: 'Too many registration attempts. Please try again after 1 hour.'
    },
    standardHeaders: true,
    legacyHeaders: false,
    skipSuccessfulRequests: false
  });
};

module.exports = {
  authLimiter: createAuthLimiter(),
  passwordResetLimiter: createPasswordResetLimiter(),
  verificationLimiter: createVerificationLimiter(),
  registrationLimiter: createRegistrationLimiter()
};
