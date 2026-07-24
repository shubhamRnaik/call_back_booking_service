-- =============================================================================
-- Voice AI Orchestrator - Multi-Tenant Booking Schema (Supabase / Postgres)
-- =============================================================================
-- Run this in the Supabase SQL Editor (or `psql` against your Supabase Postgres
-- connection string). Requires the pgcrypto extension for gen_random_uuid()
-- (enabled by default on Supabase projects).
-- =============================================================================

-- Cleanup
DROP TABLE IF EXISTS appointments CASCADE;
DROP TABLE IF EXISTS doctors_or_services CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;

-- 1. Tenants Table
CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY,
    business_name TEXT NOT NULL,
    tenant_type TEXT NOT NULL CHECK (tenant_type IN ('clinic', 'parlour')),
    timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata',
    emergency_number TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Doctors & Services Table
CREATE TABLE doctors_or_services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    qualifications TEXT,
    bio TEXT,
    price_str TEXT,
    working_hours TEXT NOT NULL,       -- e.g. '17:00-22:00'
    slot_duration_mins INT DEFAULT 30,
    status TEXT DEFAULT 'AVAILABLE' CHECK (status IN ('AVAILABLE', 'ON_LEAVE', 'FULLY_BOOKED')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Appointments Table
CREATE TABLE appointments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    item_id UUID REFERENCES doctors_or_services(id) ON DELETE CASCADE,
    item_name TEXT NOT NULL,
    date_str TEXT NOT NULL,             -- YYYY-MM-DD
    start_time_mins INT NOT NULL,       -- minutes from midnight
    end_time_mins INT NOT NULL,
    display_time_str TEXT NOT NULL,     -- e.g. '06:00 PM'
    patient_name TEXT NOT NULL,
    patient_phone TEXT NOT NULL,
    -- idempotency_key is scoped to a single booking ATTEMPT (call_id + nonce),
    -- NOT to (tenant, item, date, time, phone) alone - that combination recurs
    -- legitimately if a caller cancels and re-books the same slot later.
    idempotency_key TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'CONFIRMED' CHECK (status IN ('CONFIRMED', 'CANCELLED', 'COMPLETED')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- HARD RACE-CONDITION GUARD - the real double-booking guarantee lives here,
-- scoped to CONFIRMED rows only so cancelled slots free up correctly.
CREATE UNIQUE INDEX uq_no_double_booking
ON appointments (tenant_id, item_name, date_str, start_time_mins)
WHERE (status = 'CONFIRMED');

CREATE INDEX idx_appointments_range_check
ON appointments (tenant_id, item_name, date_str, start_time_mins, end_time_mins)
WHERE (status = 'CONFIRMED');

CREATE INDEX idx_doctors_lookup ON doctors_or_services (tenant_id, name);

-- =============================================================================
-- SEED DATA
-- =============================================================================
INSERT INTO tenants (tenant_id, business_name, tenant_type, timezone, emergency_number, system_prompt) VALUES
('CLINIC_001', 'City Health Clinic', 'clinic', 'Asia/Kolkata', '+919999999999', 'You are the receptionist for City Health Clinic. Help callers inquire about doctors and book appointments concisely in under 2 sentences.'),
('PARLOUR_001', 'Glow & Shine Beauty Parlour', 'parlour', 'Asia/Kolkata', '+919876543210', 'You are Priya, AI receptionist for Glow & Shine Beauty Parlour. Assist callers with services and bookings concisely in under 2 sentences.');

INSERT INTO doctors_or_services (tenant_id, name, category, qualifications, bio, price_str, working_hours, slot_duration_mins) VALUES
('CLINIC_001', 'Dr. Sharma', 'Cardiologist', 'MD, PhD in Cardiology from AIIMS Delhi', 'Dr. Sharma is a senior heart specialist with 18 years of clinical experience specializing in preventive cardiology and heart health. Consultation fee is 800 rupees.', '800 Rupees', '17:00-22:00', 30),
('PARLOUR_001', 'Hair Spa', 'Hair Care', 'Certified Scalp Specialist', 'Deep conditioning organic hair spa treatment that revitalizes damaged hair, reduces hair fall, and adds shine. Takes 45 minutes.', '1200 Rupees', '10:00-20:00', 45),
('PARLOUR_001', 'Gold Facial', 'Skin Care', 'Licensed Aesthetician', 'Premium 24K gold foil facial treatment for deep skin rejuvenation, tan removal, and glow.', '1500 Rupees', '10:00-20:00', 60);
