--
-- PostgreSQL database dump
--

\restrict 5mbOShU1UgRUtG0oDX7Nak4QJZn4oDYoW1KBGvTNLemOrtltuGLFg5BiWu0beXf

-- Dumped from database version 15.18 (Debian 15.18-1.pgdg13+1)
-- Dumped by pg_dump version 15.18 (Debian 15.18-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accounts (
    id uuid NOT NULL,
    account_name text NOT NULL,
    balance bigint NOT NULL,
    currency text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    account_type text DEFAULT 'USER'::text NOT NULL,
    allow_negative_balance boolean DEFAULT false NOT NULL,
    CONSTRAINT accounts_account_type_check CHECK ((account_type = ANY (ARRAY['USER'::text, 'SYSTEM'::text, 'PROMOTION_POOL'::text])))
);


--
-- Name: companies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.companies (
    id uuid NOT NULL,
    name character varying(50) NOT NULL,
    business_name character varying(50) NOT NULL,
    email_id character varying(50),
    phone_number character varying(20),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);


--
-- Name: idempotency_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.idempotency_keys (
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    response_body jsonb,
    status text NOT NULL,
    error_code text,
    error_message text,
    owner_token text,
    payment_id uuid,
    locked_until timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT idempotency_keys_status_check CHECK ((status = ANY (ARRAY['IN_PROGRESS'::text, 'COMPLETED'::text, 'FAILED'::text])))
);


--
-- Name: offer_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_events (
    id uuid NOT NULL,
    offer_id uuid NOT NULL,
    event_type character varying(50) NOT NULL,
    actor_id uuid,
    actor_type character varying(20) NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp without time zone NOT NULL,
    CONSTRAINT offer_events_actor_type_check CHECK (((actor_type)::text = ANY ((ARRAY['USER'::character varying, 'ACCOUNT'::character varying, 'SYSTEM'::character varying])::text[])))
);


--
-- Name: offer_idempotency_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_idempotency_keys (
    idempotency_key character varying(255) NOT NULL,
    offer_id uuid,
    request_hash character varying(255) NOT NULL,
    status character varying(20) NOT NULL,
    response_body jsonb,
    error_code text,
    error_message text,
    owner_token character varying(255),
    locked_until timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT offer_idempotency_keys_status_check CHECK (((status)::text = ANY ((ARRAY['IN_PROGRESS'::character varying, 'COMPLETED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: offer_outbox_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_outbox_events (
    id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    idempotency_key text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL,
    locked_until timestamp without time zone,
    retry_count integer DEFAULT 0 NOT NULL,
    trace_id text NOT NULL,
    request_id text NOT NULL,
    error_code text,
    error_message text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    published_at timestamp without time zone,
    CONSTRAINT offer_outbox_events_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'PROCESSING'::text, 'PUBLISHED'::text, 'FAILED'::text])))
);


--
-- Name: offer_redemption_idempotency_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_redemption_idempotency_keys (
    idempotency_key character varying(255) NOT NULL,
    offer_id uuid,
    payment_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    redemption_id uuid,
    request_hash character varying(255) NOT NULL,
    status character varying(20) NOT NULL,
    response_body jsonb,
    error_code text,
    error_message text,
    owner_token character varying(255),
    locked_until timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT offer_redemption_idempotency_keys_status_check CHECK (((status)::text = ANY ((ARRAY['IN_PROGRESS'::character varying, 'COMPLETED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: offer_redemptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_redemptions (
    id uuid NOT NULL,
    offer_id uuid NOT NULL,
    account_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    payment_id uuid,
    idempotency_key character varying(255) NOT NULL,
    status character varying(20) NOT NULL,
    refund_reason text,
    refunded_by_id uuid,
    refunded_by_type character varying(20),
    refund_source character varying(30),
    created_at timestamp without time zone NOT NULL,
    redeemed_at timestamp without time zone,
    refunded_at timestamp without time zone,
    CONSTRAINT offer_redemptions_refund_source_check CHECK (((refund_source)::text = ANY ((ARRAY['CUSTOMER_REQUEST'::character varying, 'FRAUD'::character varying, 'MANUAL_ADMIN'::character varying, 'SYSTEM'::character varying])::text[]))),
    CONSTRAINT offer_redemptions_refunded_by_type_check CHECK (((refunded_by_type)::text = ANY ((ARRAY['ACCOUNT'::character varying, 'USER'::character varying, 'SYSTEM'::character varying])::text[]))),
    CONSTRAINT offer_redemptions_status_check CHECK (((status)::text = ANY ((ARRAY['IN_PROGRESS'::character varying, 'SUCCESS'::character varying, 'FAILED'::character varying, 'REFUNDED'::character varying])::text[])))
);


--
-- Name: offer_reservation_idempotency_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_reservation_idempotency_keys (
    idempotency_key character varying(255) NOT NULL,
    offer_id uuid,
    reservation_id uuid,
    payment_id uuid NOT NULL,
    request_hash character varying(255) NOT NULL,
    status character varying(20) NOT NULL,
    response_body jsonb,
    error_code text,
    error_message text,
    owner_token character varying(255),
    locked_until timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT offer_reservation_idempotency_keys_status_check CHECK (((status)::text = ANY ((ARRAY['IN_PROGRESS'::character varying, 'COMPLETED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: offer_reservations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_reservations (
    id uuid NOT NULL,
    offer_id uuid NOT NULL,
    account_id uuid NOT NULL,
    payment_id uuid,
    idempotency_key character varying(255) NOT NULL,
    status character varying(20) NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT offer_reservations_status_check CHECK (((status)::text = ANY ((ARRAY['RESERVED'::character varying, 'REDEEMED'::character varying, 'CANCELLED'::character varying, 'EXPIRED'::character varying])::text[])))
);


--
-- Name: offers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offers (
    id uuid NOT NULL,
    offer_code character varying(100) NOT NULL,
    offer_type character varying(30) NOT NULL,
    offer_amount bigint,
    offer_percentage integer,
    max_benefit_amount bigint,
    minimum_payment_amount bigint,
    maximum_payment_amount bigint,
    max_redemptions integer NOT NULL,
    redeemed_count integer DEFAULT 0 NOT NULL,
    reserved_count integer DEFAULT 0 NOT NULL,
    max_redemptions_per_user integer DEFAULT 1 NOT NULL,
    idempotency_key character varying(255),
    status character varying(20) NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    created_by uuid NOT NULL,
    promotion_pool_account_id uuid,
    budget_amount bigint,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT chk_budget_positive CHECK (((budget_amount IS NULL) OR (budget_amount > 0))),
    CONSTRAINT chk_offer_amount_positive CHECK (((offer_amount IS NULL) OR (offer_amount > 0))),
    CONSTRAINT chk_offer_percentage CHECK (((offer_percentage IS NULL) OR ((offer_percentage > 0) AND (offer_percentage <= 100)))),
    CONSTRAINT chk_offer_window CHECK ((start_time < end_time)),
    CONSTRAINT chk_redemption_limit CHECK ((redeemed_count <= max_redemptions)),
    CONSTRAINT offers_check CHECK ((((offer_amount IS NOT NULL) AND (offer_percentage IS NULL)) OR ((offer_amount IS NULL) AND (offer_percentage IS NOT NULL)))),
    CONSTRAINT offers_offer_type_check CHECK (((offer_type)::text = ANY ((ARRAY['DISCOUNT'::character varying, 'CASHBACK'::character varying])::text[]))),
    CONSTRAINT offers_status_check CHECK (((status)::text = ANY ((ARRAY['DRAFT'::character varying, 'ACTIVE'::character varying, 'PAUSED'::character varying, 'EXPIRED'::character varying, 'DELETED'::character varying])::text[])))
);


--
-- Name: outbox_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_events (
    id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    idempotency_key text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL,
    locked_until timestamp without time zone,
    retry_count integer DEFAULT 0,
    trace_id text NOT NULL,
    request_id text NOT NULL,
    error_code text,
    error_message text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    published_at timestamp without time zone,
    CONSTRAINT outbox_events_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'PROCESSING'::text, 'PUBLISHED'::text, 'FAILED'::text])))
);


--
-- Name: payments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payments (
    id uuid NOT NULL,
    sender_id uuid NOT NULL,
    amount bigint NOT NULL,
    currency text NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    idempotency_key text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    receiver_id uuid NOT NULL,
    offer_id uuid,
    reservation_id uuid,
    redemption_id uuid,
    offer_benefit_amount bigint,
    offer_type text,
    offer_code text,
    net_amount bigint NOT NULL,
    offer_amount bigint,
    CONSTRAINT chk_payment_status CHECK ((status = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'FAILED'::text]))),
    CONSTRAINT payments_amount_check CHECK ((amount > 0)),
    CONSTRAINT payments_offer_type_check CHECK ((offer_type = ANY (ARRAY['DISCOUNT'::text, 'CASHBACK'::text]))),
    CONSTRAINT payments_status_check CHECK ((status = ANY (ARRAY['NONE'::text, 'RESERVED'::text, 'REDEEMED'::text, 'EXPIRED'::text])))
);


--
-- Name: transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.transactions (
    id uuid NOT NULL,
    payment_id uuid NOT NULL,
    account_id uuid NOT NULL,
    type text NOT NULL,
    amount bigint NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    transaction_category character varying(50) DEFAULT 'PAYMENT'::character varying NOT NULL,
    CONSTRAINT transactions_amount_check CHECK ((amount > 0)),
    CONSTRAINT transactions_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'FAILED'::text]))),
    CONSTRAINT transactions_transaction_category_check CHECK (((transaction_category)::text = ANY ((ARRAY['PAYMENT'::character varying, 'CASHBACK'::character varying, 'REFUND'::character varying, 'REVERSAL'::character varying, 'FEE'::character varying])::text[]))),
    CONSTRAINT transactions_type_check CHECK ((type = ANY (ARRAY['DEBIT'::text, 'CREDIT'::text])))
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    company_id uuid,
    role character varying(20) NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT users_role_check CHECK (((role)::text = ANY ((ARRAY['ADMIN'::character varying, 'USER'::character varying])::text[])))
);


--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: idempotency_keys idempotency_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.idempotency_keys
    ADD CONSTRAINT idempotency_keys_pkey PRIMARY KEY (idempotency_key);


--
-- Name: offer_events offer_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_events
    ADD CONSTRAINT offer_events_pkey PRIMARY KEY (id);


--
-- Name: offer_idempotency_keys offer_idempotency_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_idempotency_keys
    ADD CONSTRAINT offer_idempotency_keys_pkey PRIMARY KEY (idempotency_key);


--
-- Name: offer_outbox_events offer_outbox_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_outbox_events
    ADD CONSTRAINT offer_outbox_events_pkey PRIMARY KEY (id);


--
-- Name: offer_redemption_idempotency_keys offer_redemption_idempotency_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemption_idempotency_keys
    ADD CONSTRAINT offer_redemption_idempotency_keys_pkey PRIMARY KEY (idempotency_key);


--
-- Name: offer_redemptions offer_redemptions_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT offer_redemptions_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: offer_redemptions offer_redemptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT offer_redemptions_pkey PRIMARY KEY (id);


--
-- Name: offer_redemptions offer_redemptions_reservation_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT offer_redemptions_reservation_id_key UNIQUE (reservation_id);


--
-- Name: offer_reservation_idempotency_keys offer_reservation_idempotency_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservation_idempotency_keys
    ADD CONSTRAINT offer_reservation_idempotency_keys_pkey PRIMARY KEY (idempotency_key);


--
-- Name: offer_reservations offer_reservations_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservations
    ADD CONSTRAINT offer_reservations_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: offer_reservations offer_reservations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservations
    ADD CONSTRAINT offer_reservations_pkey PRIMARY KEY (id);


--
-- Name: offers offers_offer_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_offer_code_key UNIQUE (offer_code);


--
-- Name: offers offers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_pkey PRIMARY KEY (id);


--
-- Name: offers offers_promotion_pool_account_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_promotion_pool_account_id_key UNIQUE (promotion_pool_account_id);


--
-- Name: outbox_events outbox_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (id);


--
-- Name: payments payments_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT payments_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: payments payments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT payments_pkey PRIMARY KEY (id);


--
-- Name: transactions transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (id);


--
-- Name: payments uq_payments_idempotency_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_idempotency_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_idempotency_status ON public.idempotency_keys USING btree (status);


--
-- Name: idx_offer_events_offer_id_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_events_offer_id_created_at ON public.offer_events USING btree (offer_id, created_at DESC);


--
-- Name: idx_offer_outbox_aggregate; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_outbox_aggregate ON public.offer_outbox_events USING btree (aggregate_type, aggregate_id);


--
-- Name: idx_offer_outbox_idempotency_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_outbox_idempotency_key ON public.offer_outbox_events USING btree (idempotency_key);


--
-- Name: idx_offer_outbox_status_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_outbox_status_created_at ON public.offer_outbox_events USING btree (status, created_at);


--
-- Name: idx_offer_reservations_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_reservations_account ON public.offer_reservations USING btree (account_id);


--
-- Name: idx_offer_reservations_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_reservations_expires_at ON public.offer_reservations USING btree (expires_at);


--
-- Name: idx_offer_reservations_offer_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_reservations_offer_status ON public.offer_reservations USING btree (offer_id, status);


--
-- Name: idx_offer_reservations_payment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_offer_reservations_payment ON public.offer_reservations USING btree (payment_id);


--
-- Name: idx_outbox_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_outbox_status ON public.outbox_events USING btree (status);


--
-- Name: idx_payments_idempotency_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_payments_idempotency_key ON public.payments USING btree (idempotency_key);


--
-- Name: idx_payments_offer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payments_offer_id ON public.payments USING btree (offer_id);


--
-- Name: idx_payments_redemption_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payments_redemption_id ON public.payments USING btree (redemption_id);


--
-- Name: idx_payments_reservation_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payments_reservation_id ON public.payments USING btree (reservation_id);


--
-- Name: idx_transactions_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transactions_account_id ON public.transactions USING btree (account_id);


--
-- Name: idx_transactions_payment_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transactions_payment_id ON public.transactions USING btree (payment_id);


--
-- Name: unique_payment_type; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX unique_payment_type ON public.transactions USING btree (payment_id, type);


--
-- Name: offer_redemptions fk_account_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES public.accounts(id);


--
-- Name: users fk_account_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES public.accounts(id);


--
-- Name: users fk_company_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES public.companies(id);


--
-- Name: offers fk_idempotency_key; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT fk_idempotency_key FOREIGN KEY (idempotency_key) REFERENCES public.offer_idempotency_keys(idempotency_key);


--
-- Name: offers fk_offer_creation; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT fk_offer_creation FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: offer_events fk_offer_event_offer; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_events
    ADD CONSTRAINT fk_offer_event_offer FOREIGN KEY (offer_id) REFERENCES public.offers(id);


--
-- Name: offer_redemptions fk_offer_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT fk_offer_id FOREIGN KEY (offer_id) REFERENCES public.offers(id);


--
-- Name: offer_redemption_idempotency_keys fk_offer_redemption_offer; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemption_idempotency_keys
    ADD CONSTRAINT fk_offer_redemption_offer FOREIGN KEY (offer_id) REFERENCES public.offers(id);


--
-- Name: offer_reservations fk_offer_reservation_account; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservations
    ADD CONSTRAINT fk_offer_reservation_account FOREIGN KEY (account_id) REFERENCES public.accounts(id);


--
-- Name: offer_reservation_idempotency_keys fk_offer_reservation_offer; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservation_idempotency_keys
    ADD CONSTRAINT fk_offer_reservation_offer FOREIGN KEY (offer_id) REFERENCES public.offers(id);


--
-- Name: offer_reservations fk_offer_reservation_offer; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservations
    ADD CONSTRAINT fk_offer_reservation_offer FOREIGN KEY (offer_id) REFERENCES public.offers(id);


--
-- Name: offers fk_promotion_pool_account; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT fk_promotion_pool_account FOREIGN KEY (promotion_pool_account_id) REFERENCES public.accounts(id);


--
-- Name: payments fk_receiver; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT fk_receiver FOREIGN KEY (receiver_id) REFERENCES public.accounts(id);


--
-- Name: offer_redemptions fk_reservation_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_redemptions
    ADD CONSTRAINT fk_reservation_id FOREIGN KEY (reservation_id) REFERENCES public.offer_reservations(id);


--
-- Name: offer_reservations fk_reservation_idempotency; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_reservations
    ADD CONSTRAINT fk_reservation_idempotency FOREIGN KEY (idempotency_key) REFERENCES public.offer_redemption_idempotency_keys(idempotency_key);


--
-- Name: payments fk_sender; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT fk_sender FOREIGN KEY (sender_id) REFERENCES public.accounts(id);


--
-- Name: transactions fk_transaction_account; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transactions
    ADD CONSTRAINT fk_transaction_account FOREIGN KEY (account_id) REFERENCES public.accounts(id);


--
-- Name: transactions fk_transaction_payment; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transactions
    ADD CONSTRAINT fk_transaction_payment FOREIGN KEY (payment_id) REFERENCES public.payments(id);


--
-- PostgreSQL database dump complete
--