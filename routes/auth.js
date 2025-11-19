const express = require('express');
const { body, validationResult } = require('express-validator');
const crypto = require('crypto');
const generateToken = require('../utils/generateToken');
const sendEmail = require('../utils/sendEmail');
const { getWelcomeEmailTemplate, getPasswordResetEmailTemplate } = require('../utils/emailTemplates');
const { protect } = require('../middleware/auth');
const { requireEmailVerification } = require('../middleware/emailVerification');
const { validateCompanyDomain } = require('../middleware/domainValidation');
const { validateEmailMiddleware } = require('../middleware/emailValidation');
const User = require('../models/User');
const Company = require('../models/Company');

const router = express.Router();

// @desc    Validate email endpoint
// @route   POST /api/auth/validate-email
// @access  Public
router.post('/validate-email', validateEmailMiddleware, async (req, res) => {
  try {
    res.json({
      success: true,
      message: 'Email is valid and can receive messages',
      data: {
        valid: true,
        reason: req.emailValidation.reason
      }
    });
  } catch (error) {
    console.error('Email validation error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during email validation'
    });
  }
});

// @desc    Register user
// @route   POST /api/auth/register
// @access  Public
router.post('/register', validateEmailMiddleware, validateCompanyDomain, [
  body('firstName')
    .trim()
    .isLength({ min: 2, max: 50 })
    .withMessage('First name must be between 2 and 50 characters'),
  body('lastName')
    .trim()
    .isLength({ min: 2, max: 50 })
    .withMessage('Last name must be between 2 and 50 characters'),
  body('email')
    .isEmail()
    .normalizeEmail()
    .withMessage('Please enter a valid email'),
  body('company')
    .trim()
    .isLength({ min: 2, max: 100 })
    .withMessage('Company name must be between 2 and 100 characters'),
  body('password')
    .isLength({ min: 6 })
    .withMessage('Password must be at least 6 characters')
    .matches(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/)
    .withMessage('Password must contain at least one uppercase letter, one lowercase letter, and one number')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const { firstName, lastName, email, company, password } = req.body;

    // Check if user already exists
    const existingUser = await User.findByEmail(email);
    if (existingUser) {
      return res.status(400).json({
        success: false,
        message: 'User already exists with this email'
      });
    }

    // Parse name from firstName and lastName
    const fullName = `${firstName} ${lastName}`.trim();

    // Create user
    const user = await User.create({
      company_id: req.approvedCompany.id,
      name: fullName,
      email,
      password
    });

    // Mark as email validated since we checked it
    const database = require('../config/database');
    await database.query('UPDATE "user" SET email_validated = true WHERE id = $1', [user.id]);

    // Generate email verification token
    const verificationToken = user.generateEmailVerificationToken();
    await user.save();

    // Create verification URL
    const verificationUrl = `http://localhost:5000/api/auth/verify-email/${verificationToken}`;

    try {
      // Send verification email
      await sendEmail({
        email: user.email,
        subject: 'Email Verification - Saher Flow Solutions',
        html: getWelcomeEmailTemplate(firstName, verificationUrl)
      });

      res.status(201).json({
        success: true,
        message: 'User registered successfully. Please check your email to verify your account.',
        data: {
          user: {
            id: user._id,
            firstName: firstName,
            lastName: lastName,
            email: user.email,
            company: req.approvedCompany.name,
            isEmailVerified: user.is_email_verified
          }
        }
      });
    } catch (emailError) {
      console.error('Verification email failed:', emailError);
      
      // Delete user if email fails to send
      await User.delete(user.id);
      
      res.status(500).json({
        success: false,
        message: 'Registration failed. Could not send verification email. Please try again.'
      });
    }
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during registration'
    });
  }
});

// @desc    Login user
// @route   POST /api/auth/login
// @access  Public


router.post('/login', [
  
  body('email')
    .isEmail()
    .normalizeEmail()
    .withMessage('Please enter a valid email'),
  body('password')
    .notEmpty()
    .withMessage('Password is required')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const { email, password } = req.body;

    // Check if user exists and get password
    const user = await User.findByEmail(email, true);
    if (!user) {
      return res.status(401).json({
        success: false,
        message: 'Invalid email or password'
      });
    }

    // Check if account is active
    if (!user.is_active) {
      return res.status(401).json({
        success: false,
        message: 'Account has been deactivated. Please contact support.'
      });
    }

    // Check password
    const isPasswordValid = await user.comparePassword(password);
    if (!isPasswordValid) {
      return res.status(401).json({
        success: false,
        message: 'Invalid email or password'
      });
    }

    // Get user's IP address
    const getUserIP = (req) => {
      return req.headers['x-forwarded-for'] || 
             req.headers['x-real-ip'] || 
             req.connection.remoteAddress || 
             req.socket.remoteAddress ||
             (req.connection.socket ? req.connection.socket.remoteAddress : null) ||
             req.ip;
    };

    const rawIP = getUserIP(req);
    
    // Convert IPv6 loopback to IPv4 for better readability
    let userIP = rawIP;
    if (rawIP === '::1' || rawIP === '::ffff:127.0.0.1') {
      userIP = '127.0.0.1'; // localhost
    } else if (rawIP && rawIP.startsWith('::ffff:')) {
      // Extract IPv4 from IPv6-mapped address
      userIP = rawIP.substring(7);
    }

    // Update last login and IP
    user.last_login_time = new Date();
    user.last_login_ip = userIP;
    await user.save();

    // Generate token
    const token = generateToken(user.id);

    res.json({
      success: true,
      message: 'Login successful',
      data: {
        token,
        user: {
          id: user.id,
          firstName: user.name.split(' ')[0] || user.name,
          lastName: user.name.split(' ').slice(1).join(' ') || '',
          email: user.email,
          company: user.company_name || '',
          role: user.role,
          isEmailVerified: user.is_email_verified,
          lastLogin: user.last_login_time,
          lastLoginIP: user.last_login_ip
        }
      }
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during login'
    });
  }
});

// @desc    Verify email
// @route   GET /api/auth/verify-email/:token
// @access  Public
router.get('/verify-email/:token', async (req, res) => {
  try {
    // Hash the token from URL
    const hashedToken = crypto
      .createHash('sha256')
      .update(req.params.token)
      .digest('hex');

    // Find user with this token using raw query
    const database = require('../config/database');
    const result = await database.query(
      'SELECT * FROM "user" WHERE email_verification_token = $1 AND email_verification_expires > $2',
      [hashedToken, new Date()]
    );
    
    const user = result.rows[0] ? new User(result.rows[0]) : null;

    if (!user) {
      return res.status(400).json({
        success: false,
        message: 'Invalid or expired verification token'
      });
    }

    // Verify email
    await database.query(
      'UPDATE "user" SET is_email_verified = true, email_verification_token = null, email_verification_expires = null, updated_at = now() WHERE id = $1',
      [user.id]
    );

    // Send HTML response for better user experience
    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Email Verified - Saher Flow Solutions</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
          .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
          .header { background: linear-gradient(135deg, #1a3a5c 0%, #153149 100%); padding: 30px; text-align: center; }
          .header h1 { color: #ffd500; margin: 0; font-size: 28px; }
          .content { padding: 30px; text-align: center; }
          .success-icon { font-size: 48px; color: #28a745; margin-bottom: 20px; }
          .btn { background: #ffd500; color: #1a3a5c; padding: 12px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block; margin-top: 20px; }
          .btn:hover { background: #e6c200; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h1>Saher Flow Solutions</h1>
          </div>
          <div class="content">
            <div class="success-icon">✅</div>
            <h2 style="color: #1a3a5c;">Email Verified Successfully!</h2>
            <p>Your email address has been verified. You can now access all features of your account.</p>
            <a href="${process.env.CLIENT_URL || 'http://localhost:5173'}" class="btn">Go to Dashboard</a>
            <p style="margin-top: 30px; color: #666; font-size: 14px;">
              You can now close this window and return to the application.
            </p>
          </div>
        </div>
      </body>
      </html>
    `);
  } catch (error) {
    console.error('Email verification error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during email verification'
    });
  }
});

// @desc    Forgot password
// @route   POST /api/auth/forgot-password
// @access  Public
router.post('/forgot-password', [
  body('email')
    .isEmail()
    .normalizeEmail()
    .withMessage('Please enter a valid email')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const { email } = req.body;

    // Find user
    const user = await User.findByEmail(email);
    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'No user found with this email address'
      });
    }

    // Generate reset token
    const resetToken = user.generatePasswordResetToken();
    await user.save({ validateBeforeSave: false });

    // Create reset URL
    const resetUrl = `http://localhost:5173/reset-password/${resetToken}`;

    try {
      // Send password reset email
      await sendEmail({
        email: user.email,
        subject: 'Password Reset Request - Saher Flow Solutions',
        html: getPasswordResetEmailTemplate(user.name.split(' ')[0] || user.name, resetUrl)
      });

      res.json({
        success: true,
        message: 'Password reset email sent successfully'
      });
    } catch (emailError) {
      console.error('Password reset email failed:', emailError);
      
      // Clear reset token if email fails
      const database = require('../config/database');
      await database.query(
        'UPDATE "user" SET password_reset_token = null, password_reset_expires = null WHERE id = $1',
        [user.id]
      );
      
      res.status(500).json({
        success: false,
        message: 'Could not send password reset email'
      });
    }
  } catch (error) {
    console.error('Forgot password error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during password reset request'
    });
  }
});

// @desc    Reset password
// @route   PUT /api/auth/reset-password/:token
// @access  Public
router.put('/reset-password/:token', [
  body('password')
    .isLength({ min: 6 })
    .withMessage('Password must be at least 6 characters')
    .matches(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/)
    .withMessage('Password must contain at least one uppercase letter, one lowercase letter, and one number')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    // Hash the token from URL
    const hashedToken = crypto
      .createHash('sha256')
      .update(req.params.token)
      .digest('hex');

    // Find user with this token using raw query
    const database = require('../config/database');
    const result = await database.query(
      'SELECT * FROM "user" WHERE password_reset_token = $1 AND password_reset_expires > $2',
      [hashedToken, new Date()]
    );
    
    const user = result.rows[0] ? new User(result.rows[0]) : null;

    if (!user) {
      return res.status(400).json({
        success: false,
        message: 'Invalid or expired reset token'
      });
    }

    // Set new password
    await user.updatePassword(req.body.password);
    
    // Clear reset tokens
    await database.query(
      'UPDATE "user" SET password_reset_token = null, password_reset_expires = null WHERE id = $1',
      [user.id]
    );

    // Generate new token
    const token = generateToken(user.id);

    res.json({
      success: true,
      message: 'Password reset successful',
      data: {
        token,
        user: {
          id: user.id,
          firstName: user.name.split(' ')[0] || user.name,
          lastName: user.name.split(' ').slice(1).join(' ') || '',
          email: user.email,
          company: user.company_name || '',
          role: user.role,
          isEmailVerified: user.is_email_verified
        }
      }
    });
  } catch (error) {
    console.error('Password reset error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during password reset'
    });
  }
});

// @desc    Resend verification email
// @route   POST /api/auth/resend-verification
// @access  Public
router.post('/resend-verification', [
  body('email')
    .isEmail()
    .normalizeEmail()
    .withMessage('Please enter a valid email')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const { email } = req.body;

    // Find user
    const user = await User.findByEmail(email);
    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'No user found with this email address'
      });
    }

    // Check if already verified
    if (user.is_email_verified) {
      return res.status(400).json({
        success: false,
        message: 'Email is already verified'
      });
    }

    // Generate new verification token
    const verificationToken = user.generateEmailVerificationToken();
    await user.save();

    // Create verification URL
    const verificationUrl = `${process.env.API_URL || 'http://localhost:5000'}/api/auth/verify-email/${verificationToken}`;

    try {
      // Send verification email
      await sendEmail({
        email: user.email,
        subject: 'Email Verification - Saher Flow Solutions',
        html: getWelcomeEmailTemplate(user.name.split(' ')[0] || user.name, verificationUrl)
      });

      res.json({
        success: true,
        message: 'Verification email sent successfully'
      });
    } catch (emailError) {
      console.error('Verification email failed:', emailError);
      res.status(500).json({
        success: false,
        message: 'Could not send verification email'
      });
    }
  } catch (error) {
    console.error('Resend verification error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during verification email resend'
    });
  }
});

// @desc    Get current user
// @route   GET /api/auth/me
// @access  Private
router.get('/me', protect, async (req, res) => {
  try {
    // Get user with company information
    const database = require('../config/database');
    const result = await database.query(`
      SELECT u.*, c.name as company_name 
      FROM "user" u 
      LEFT JOIN company c ON u.company_id = c.id 
      WHERE u.id = $1
    `, [req.user.id]);
    
    if (!result.rows[0]) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }
    
    const userData = result.rows[0];
    const user = {
      id: userData.id,
      firstName: userData.name.split(' ')[0] || userData.name,
      lastName: userData.name.split(' ').slice(1).join(' ') || '',
      email: userData.email,
      company: userData.company_name || '',
      role: userData.role,
      isEmailVerified: userData.is_email_verified,
      isActive: userData.is_active
    };

    res.json({
      success: true,
      data: {
        user
      }
    });
  } catch (error) {
    console.error('Get current user error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting user data'
    });
  }
});

// @desc    Logout user
// @route   POST /api/auth/logout
// @access  Private
router.post('/logout', protect, async (req, res) => {
  try {
    // In a stateless JWT system, logout is handled client-side
    // But we can track logout time if needed
    res.json({
      success: true,
      message: 'Logged out successfully'
    });
  } catch (error) {
    console.error('Logout error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error during logout'
    });
  }
});

module.exports = router;