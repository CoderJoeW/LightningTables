-- Seed users table
INSERT INTO users (first_name, last_name) VALUES ('John', 'Doe');
INSERT INTO users (first_name, last_name) VALUES ('Jane', 'Smith');
INSERT INTO users (first_name, last_name) VALUES ('Bob', 'Johnson');

-- Seed transactions table
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 1.25);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 0.05);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 0.50);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'CREDIT', 'CALL', 5.00);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 1.75);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'CALL', 0.95);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'SMS', 0.10);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'DATA', 0.80);
INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'DEBIT', 'CALL', 1.50);
INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'CREDIT', 'SMS', 2.00);

