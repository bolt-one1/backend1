const express = require('express');
const { body, validationResult } = require('express-validator');
const Company = require('../models/Company');
const { protect, admin } = require('../middleware/auth');

const router = express.Router();

// @desc    Get all companies
// @route   GET /api/company
// @access  Public (for registration domain checking)
router.get('/', async (req, res) => {
  try {
    const companies = await Company.findAll();

    res.json({
      success: true,
      data: {
        companies: companies.map(company => company.toJSON())
      }
    });
  } catch (error) {
    console.error('Get companies error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting companies'
    });
  }
});

// @desc    Get company by ID
// @route   GET /api/company/:id
// @access  Private/Admin
router.get('/:id', protect, admin, async (req, res) => {
  try {
    const company = await Company.findById(parseInt(req.params.id));

    if (!company) {
      return res.status(404).json({
        success: false,
        message: 'Company not found'
      });
    }

    res.json({
      success: true,
      data: {
        company: company.toJSON()
      }
    });
  } catch (error) {
    console.error('Get company error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting company'
    });
  }
});

// @desc    Create new company
// @route   POST /api/company
// @access  Private/Admin
router.post('/', protect, admin, [
  body('name')
    .trim()
    .isLength({ min: 2, max: 100 })
    .withMessage('Company name must be between 2 and 100 characters'),
  body('domain_name')
    .notEmpty()
    .withMessage('Domain is required'),
  body('domains')
    .optional()
    .isArray()
    .withMessage('Domains must be an array')
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

    const { name, domain_name, domains } = req.body;
    
    // Use domain_name or first domain from domains array for compatibility
    const finalDomain = domain_name || (domains && domains[0]);
    
    if (!finalDomain) {
      return res.status(400).json({
        success: false,
        message: 'Domain is required'
      });
    }

    // Check if company name already exists
    const nameExists = await Company.checkNameExists(name);
    if (nameExists) {
      return res.status(400).json({
        success: false,
        message: 'Company with this name already exists'
      });
    }

    // Check if domain is already used
    const domainExists = await Company.checkDomainExists(finalDomain);
    if (domainExists) {
      return res.status(400).json({
        success: false,
        message: `Domain ${finalDomain} is already registered`
      });
    }

    // Create company
    const company = await Company.create({
      name,
      domain_name: finalDomain
    });

    res.status(201).json({
      success: true,
      message: 'Company created successfully',
      data: {
        company: company.toJSON()
      }
    });
  } catch (error) {
    console.error('Create company error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error creating company'
    });
  }
});

// @desc    Update company
// @route   PUT /api/company/:id
// @access  Private/Admin
router.put('/:id', protect, admin, [
  body('name')
    .optional()
    .trim()
    .isLength({ min: 2, max: 100 })
    .withMessage('Company name must be between 2 and 100 characters'),
  body('domain_name')
    .optional()
    .notEmpty()
    .withMessage('Domain cannot be empty'),
  body('domains')
    .optional()
    .isArray()
    .withMessage('Domains must be an array')
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

    const { name, domain_name, domains } = req.body;
    const id = parseInt(req.params.id);

    // Find company
    const company = await Company.findById(id);
    if (!company) {
      return res.status(404).json({
        success: false,
        message: 'Company not found'
      });
    }

    // Check if new name conflicts with existing companies
    if (name && name !== company.name) {
      const nameExists = await Company.checkNameExists(name, id);
      if (nameExists) {
        return res.status(400).json({
          success: false,
          message: 'Company with this name already exists'
        });
      }
    }

    // Check if new domain conflicts with existing companies
    const finalDomain = domain_name || (domains && domains[0]);
    if (finalDomain && finalDomain !== company.domain_name) {
      const domainExists = await Company.checkDomainExists(finalDomain, id);
      if (domainExists) {
        return res.status(400).json({
          success: false,
          message: `Domain ${finalDomain} is already registered`
        });
      }
    }

    // Update company
    const updateData = {};
    if (name) updateData.name = name;
    if (finalDomain) updateData.domain_name = finalDomain;
    
    const updatedCompany = await Company.update(id, updateData);

    res.json({
      success: true,
      message: 'Company updated successfully',
      data: {
        company: updatedCompany.toJSON()
      }
    });
  } catch (error) {
    console.error('Update company error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error updating company'
    });
  }
});

// @desc    Delete company (soft delete)
// @route   DELETE /api/company/:id
// @access  Private/Admin
router.delete('/:id', protect, admin, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const company = await Company.findById(id);
    
    if (!company) {
      return res.status(404).json({
        success: false,
        message: 'Company not found'
      });
    }

    // Hard delete company (this will cascade delete users)
    await Company.delete(id);

    res.json({
      success: true,
      message: 'Company deactivated successfully'
    });
  } catch (error) {
    console.error('Delete company error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error deleting company'
    });
  }
});

// @desc    Check if domain is allowed
// @route   GET /api/company/check-domain/:domain
// @access  Public
router.get('/check-domain/:domain', async (req, res) => {
  try {
    const domain = req.params.domain.toLowerCase();
    
    const company = await Company.findByDomain(domain);
    
    res.json({
      success: true,
      data: {
        isAllowed: !!company,
        company: company ? {
          id: company.id,
          name: company.name,
          domain_name: company.domain_name
        } : null
      }
    });
  } catch (error) {
    console.error('Check domain error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error checking domain'
    });
  }
});

module.exports = router;