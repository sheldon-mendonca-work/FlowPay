-- Payment <-> Offer consistency checks
--
-- Note: payments.offer_id/reservation_id/redemption_id are never populated by
-- payment-service (checked: no INSERT/UPDATE writes them - see
-- payment_repository.go CreatePayment). The reliable link is
-- offer_reservations.payment_id / offer_redemptions.payment_id, written by
-- offer-service when it consumes the PaymentInitiated/PaymentSuccess events.
-- Checks 1 and 2 below join through that direction, not payments.offer_id.

-- 1. Successful payments whose linked reservation was never marked REDEEMED.
SELECT
    p.id AS payment_id,
    res.id AS reservation_id,
    res.offer_id,
    res.status AS reservation_status,
    p.status AS payment_status
FROM payments p
JOIN offer_reservations res
    ON res.payment_id = p.id
WHERE p.status = 'SUCCESS'
AND res.status != 'REDEEMED';

-- 2. Reservations marked REDEEMED whose linked payment did not succeed.
SELECT
    res.id AS reservation_id,
    res.offer_id,
    p.id AS payment_id,
    res.status AS reservation_status,
    p.status AS payment_status
FROM offer_reservations res
JOIN payments p
    ON p.id = res.payment_id
WHERE res.status = 'REDEEMED'
AND p.status != 'SUCCESS';

-- 3. Offer reservations pointing at a payment that does not exist.
SELECT
    res.id AS reservation_id,
    res.offer_id,
    res.payment_id,
    res.status
FROM offer_reservations res
WHERE res.payment_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM payments p WHERE p.id = res.payment_id
);

-- 4. Offer redemptions pointing at a payment that does not exist.
SELECT
    red.id AS redemption_id,
    red.offer_id,
    red.reservation_id,
    red.payment_id,
    red.status
FROM offer_redemptions red
WHERE red.payment_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM payments p WHERE p.id = red.payment_id
);

-- 5. Reservations still RESERVED past their expiry (missed by offer-expiry-worker).
SELECT
    res.id AS reservation_id,
    res.offer_id,
    res.payment_id,
    res.expires_at
FROM offer_reservations res
WHERE res.status = 'RESERVED'
AND res.expires_at < NOW();

-- 6. Reservation status disagrees with its redemption status.
SELECT
    res.id AS reservation_id,
    res.offer_id,
    res.status AS reservation_status,
    red.id AS redemption_id,
    red.status AS redemption_status
FROM offer_reservations res
LEFT JOIN offer_redemptions red
    ON red.reservation_id = res.id
WHERE (res.status = 'REDEEMED' AND (red.id IS NULL OR red.status != 'SUCCESS'))
OR (red.status = 'SUCCESS' AND res.status != 'REDEEMED');

-- 7. Offer redeemed_count counter drifted from actual successful redemptions.
SELECT
    o.id AS offer_id,
    o.offer_code,
    o.redeemed_count,
    COUNT(red.id) FILTER (WHERE red.status = 'SUCCESS') AS actual_redeemed_count
FROM offers o
LEFT JOIN offer_redemptions red
    ON red.offer_id = o.id
GROUP BY o.id, o.offer_code, o.redeemed_count
HAVING o.redeemed_count != COUNT(red.id) FILTER (WHERE red.status = 'SUCCESS');
