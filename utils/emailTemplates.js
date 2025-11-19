const getWelcomeEmailTemplate = (firstName, verificationUrl) => {
  return `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Welcome to Saher Flow Solutions</title>
    </head>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
      <div style="background: linear-gradient(135deg, #1a3a5c 0%, #153149 100%); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
        <h1 style="color: #ffd500; margin: 0; font-size: 28px;">Welcome to Saher Flow Solutions</h1>
        <p style="color: #ffffff; margin: 10px 0 0 0; font-size: 16px;">Professional Flow Measurement Platform</p>
      </div>
      
      <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none;">
        <h2 style="color: #1a3a5c; margin-top: 0;">Hello ${firstName}!</h2>
        
        <p>Thank you for joining Saher Flow Solutions. We're excited to have you on board!</p>
        
        <p>To get started, please verify your email address by clicking the button below:</p>
        
        <div style="text-align: center; margin: 30px 0;">
          <a href="${verificationUrl}" style="background: #ffd500; color: #1a3a5c; padding: 12px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;" target="_blank">Verify Email Address</a>
        </div>
        
        <p>Or copy and paste this link into your browser:</p>
        <p style="word-break: break-all; color: #666; font-size: 14px;">${verificationUrl}</p>
        
        <p><strong>This verification link will expire in 24 hours.</strong></p>
        
        <div style="background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0;">
          <p style="margin: 0; color: #666; font-size: 14px;">
            <strong>Important:</strong> This link will open in your browser and redirect you back to the application after verification.
          </p>
        </div>
        
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 30px 0;">
        
        <p style="color: #666; font-size: 14px;">
          If you didn't create an account with us, please ignore this email.
        </p>
        
        <p style="color: #666; font-size: 14px;">
          Best regards,<br>
          The Saher Flow Solutions Team
        </p>
      </div>
    </body>
    </html>
  `;
};

const getPasswordResetEmailTemplate = (firstName, resetUrl) => {
  return `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Reset Your Password - Saher Flow Solutions</title>
    </head>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
      <div style="background: linear-gradient(135deg, #1a3a5c 0%, #153149 100%); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
        <h1 style="color: #ffd500; margin: 0; font-size: 28px;">Password Reset Request</h1>
        <p style="color: #ffffff; margin: 10px 0 0 0; font-size: 16px;">Saher Flow Solutions</p>
      </div>
      
      <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none;">
        <h2 style="color: #1a3a5c; margin-top: 0;">Hello ${firstName}!</h2>
        
        <p>We received a request to reset your password for your Saher Flow Solutions account.</p>
        
        <p>Click the button below to reset your password:</p>
        
        <div style="text-align: center; margin: 30px 0;">
          <a href="${resetUrl}" style="background: #ffd500; color: #1a3a5c; padding: 12px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;" target="_blank">Reset Password</a>
        </div>
        
        <p>Or copy and paste this link into your browser:</p>
        <p style="word-break: break-all; color: #666; font-size: 14px;">${resetUrl}</p>
        
        <p><strong>This reset link will expire in 10 minutes for security reasons.</strong></p>
        
        <div style="background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0;">
          <p style="margin: 0; color: #666; font-size: 14px;">
            <strong>Important:</strong> This link will open in your browser where you can set your new password.
          </p>
        </div>
        
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 30px 0;">
        
        <p style="color: #666; font-size: 14px;">
          If you didn't request a password reset, please ignore this email. Your password will remain unchanged.
        </p>
        
        <p style="color: #666; font-size: 14px;">
          Best regards,<br>
          The Saher Flow Solutions Team
        </p>
      </div>
    </body>
    </html>
  `;
};

module.exports = {
  getWelcomeEmailTemplate,
  getPasswordResetEmailTemplate
};