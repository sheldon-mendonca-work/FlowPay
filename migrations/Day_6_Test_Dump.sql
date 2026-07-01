--companies

INSERT INTO companies (
    id,
    name,
    business_name,
    email_id,
    phone_number,
    created_at,
    updated_at
)
VALUES
(
    '11111111-1111-1111-1111-111111111111',
    'Amazon',
    'Amazon India Pvt Ltd',
    'offers@amazon.com',
    '+919999999001',
    NOW(),
    NOW()
),
(
    '22222222-2222-2222-2222-222222222222',
    'Flipkart',
    'Flipkart Internet Pvt Ltd',
    'offers@flipkart.com',
    '+919999999002',
    NOW(),
    NOW()
);


--accounts
INSERT INTO accounts (
    id,
    account_name,
    balance,
    currency,
    account_type,
    allow_negative_balance,
    created_at,
    updated_at
)
VALUES
('a1000000-0000-0000-0000-000000000001','Rahul',150000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000002','Priya',250000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000003','Amit',300000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000004','Sneha',120000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000005','Vikram',500000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000006','Neha',80000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000007','Arjun',450000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000008','Kavya',90000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000009','Rohan',220000,'INR','USER',false,NOW(),NOW()),
('a1000000-0000-0000-0000-000000000010','Meera',175000,'INR','USER',false,NOW(),NOW());

INSERT INTO accounts (
    id,
    account_name,
    balance,
    currency,
    account_type,
    allow_negative_balance,
    created_at,
    updated_at
)
VALUES
(
    'b1000000-0000-0000-0000-000000000001',
    'Amazon_AMZ100_cashback_pool',
    5000000,
    'INR',
    'PROMOTION_POOL',
    true,
    NOW(),
    NOW()
),
(
    'b1000000-0000-0000-0000-000000000002',
    'Amazon_AMZ20_discount_pool',
    3000000,
    'INR',
    'PROMOTION_POOL',
    true,
    NOW(),
    NOW()
),
(
    'b1000000-0000-0000-0000-000000000003',
    'Flipkart_FLIP15_discount_pool',
    4000000,
    'INR',
    'PROMOTION_POOL',
    true,
    NOW(),
    NOW()
),
(
    'b1000000-0000-0000-0000-000000000004',
    'Flipkart_FLIP200_cashback_pool',
    6000000,
    'INR',
    'PROMOTION_POOL',
    true,
    NOW(),
    NOW()
);

-- defaultcredentials
INSERT INTO defaultcredentials (account_id, pdisplay_name, description, created_at, updated_at)
VALUES
('a1000000-0000-0000-0000-000000000001', 'Rahul',  'Amazon Admin',   NOW(), NOW()),
('a1000000-0000-0000-0000-000000000002', 'Priya',  'Amazon User',    NOW(), NOW()),
('a1000000-0000-0000-0000-000000000003', 'Amit',   'Amazon User',    NOW(), NOW()),
('a1000000-0000-0000-0000-000000000004', 'Sneha',  'Amazon User',    NOW(), NOW()),
('a1000000-0000-0000-0000-000000000005', 'Vikram', 'Amazon User',    NOW(), NOW()),
('a1000000-0000-0000-0000-000000000006', 'Neha',   'Flipkart Admin', NOW(), NOW()),
('a1000000-0000-0000-0000-000000000007', 'Arjun',  'Flipkart User',  NOW(), NOW()),
('a1000000-0000-0000-0000-000000000008', 'Kavya',  'Flipkart User',  NOW(), NOW()),
('a1000000-0000-0000-0000-000000000009', 'Rohan',  'Flipkart User',  NOW(), NOW()),
('a1000000-0000-0000-0000-000000000010', 'Meera',  'Flipkart User',  NOW(), NOW());

-- users (3 accounts linked to companies; remaining 7 accounts are standalone)
INSERT INTO users (
    id,
    account_id,
    company_id,
    role,
    created_at,
    updated_at
)
VALUES
(
    'c1000000-0000-0000-0000-000000000001',
    'a1000000-0000-0000-0000-000000000001',
    '11111111-1111-1111-1111-111111111111',
    'ADMIN',
    NOW(),
    NOW()
),
(
    'c1000000-0000-0000-0000-000000000002',
    'a1000000-0000-0000-0000-000000000002',
    '11111111-1111-1111-1111-111111111111',
    'USER',
    NOW(),
    NOW()
),
(
    'c1000000-0000-0000-0000-000000000006',
    'a1000000-0000-0000-0000-000000000006',
    '22222222-2222-2222-2222-222222222222',
    'ADMIN',
    NOW(),
    NOW()
);