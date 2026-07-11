-- Day 12: fix offer_reservations idempotency FK pointing at the wrong table.
--
-- fk_reservation_idempotency (Day_6) references offer_redemption_idempotency_keys,
-- but the reservation flow claims its idempotency key in
-- offer_reservation_idempotency_keys before inserting into offer_reservations.
-- Every reservation insert violates the FK (23503) because the key only
-- ever exists in the reservation table, never the redemption one.
ALTER TABLE offer_reservations
    DROP CONSTRAINT fk_reservation_idempotency;

ALTER TABLE offer_reservations
    ADD CONSTRAINT fk_reservation_idempotency
    FOREIGN KEY (idempotency_key)
    REFERENCES offer_reservation_idempotency_keys(idempotency_key);
