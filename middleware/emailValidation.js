const { validateEmailDomain, isDisposableEmail } = require('../utils/emailValidator');

// Middleware to validate email in real-time
const validateEmailMiddleware = async (req, res, next) => {
  try {
    const { email } = req.body;
    
    if (!email) {
      return res.status(400).json({
        success: false,
        message: 'Email is required'
      });
    }

    // Check if it's a disposable email
    if (isDisposableEmail(email)) {
      return res.status(400).json({
        success: false,
        message: 'Disposable email addresses are not allowed'
      });
    }

    // Validate email domain with live DNS check
    const validation = await validateEmailDomain(email);
    
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        message: `Email validation failed: ${validation.reason}`
      });
    }

    // Store validation result for later use
    req.emailValidation = validation;
    next();
  } catch (error) {
    console.error('Email validation middleware error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during email validation'
    });
  }
};

module.exports = { validateEmailMiddleware };