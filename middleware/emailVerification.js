const User = require('../models/User');

// Middleware to check if email is verified for protected routes
const requireEmailVerification = async (req, res, next) => {
  try {
    if (!req.user.is_email_verified) {
      return res.status(403).json({
        success: false,
        message: 'Please verify your email address before accessing this resource. Check your inbox for the verification email.'
      });
    }
    next();
  } catch (error) {
    console.error('Email verification middleware error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error checking email verification'
    });
  }
};

module.exports = { requireEmailVerification };