# Payme WhatsApp-Driven Remittance Orchestrator

> **Enterprise Conversational Remittance Platform built on Meta WhatsApp Cloud API (v23.0+), Encrypted WhatsApp Flows (v7.0), Hybrid AI Intent Routing, and Existing Payme Core Services.**

`payme-whatsapp-orchestrator` is the production backend service that powers **Payme's WhatsApp Money Transfer Channel**—modeled on proven industry architectures from [Ria Money Transfer](https://www.riamoneytransfer.com/en/blog/how-to-send-money-on-whatsapp/), [Félix Pago](https://stripe.com/customers/felix) ([Tempo Case Study](https://tempo.xyz/customer-stories/felix-pago/)), and [Remitly](https://www.remitly.com/blog/money-transfer/how-to-send-money-with-remitly-using-whatsapp/).

It enables customers to initiate cross-border remittances from `payme.com` (via pre-seeded `wa.me` deep links), calculate live FX quotes, select payout methods (**Instant Bank Deposit**, **Cash Pickup**, **Mobile Wallet**), and submit beneficiary details inside encrypted **WhatsApp Flows**, while handing off sensitive **KYC verification** and **PCI-DSS 3D Secure 2.0 payment capture** to Payme's existing web checkout inside WhatsApp's In-App Browser (WebView).

## 1. System Highlights & Core Capabilities

| Capability | Technical Implementation | Business & Compliance Guarantee |
| :--- | :--- | :--- |
| **Website-to-WhatsApp Handoff** | `POST /v1/wa-handoff/init` mints a 15-min single-use `PMW-xxxxxx` token embedded in `wa.me` link | Preserves website calculator corridor/amount & binds logged-in web users to their WhatsApp MSISDN |
| **2-Hour Active Session Engine** | Redis 7 hot cache (`payme:wa:session:{wa_phone}`, TTL `7200s`) synced to PostgreSQL 16 `wa_sessions` | Supports seamless pause/resume (`▶️ Resume Current` vs. `🔄 Start New`) if the user re-clicks from the website |
| **Encrypted In-Chat Forms** | [WhatsApp Flows JSON v7.0](https://docs.kapso.ai/docs/whatsapp/flows/flow-json) + `RSA-OAEP-SHA256` & `AES-128-GCM` (`~IV`) endpoint | Collects beneficiary bank/CLABE/cash-pickup details natively inside WhatsApp without plain-text chat PII |
| **Zero-PAN PCI-DSS Handoff** | Signed short-lived JWT (`cta_url`) opening Payme's existing KYC & 3DS2 Checkout in WhatsApp WebView | Raw card numbers (PAN/CVV) and ID selfies never enter WhatsApp chat logs ([Ria Safety Standard](https://help.riamoneytransfer.com/hc/en-us/articles/44590303358353-Is-it-safe-to-send-money-via-WhatsApp)) |
| **Window-Aware Outbound Engine** | BullMQ worker checks `service_window_expires_at > NOW()` ([Meta 24h Service Window](https://developers.facebook.com/documentation/business-messaging/whatsapp/messages/send-messages)) | Sends free-form `interactive` JSON inside 24h window; auto-falls back to `APPROVED` Utility Templates outside 24h |
| **Optional AI & Voice Layer** | Hybrid NLU / ASR (Whisper + Gemini Structured Outputs) + Vision OCR | Extracts intent from multilingual voice notes/text & bank slip photos while deterministic APIs lock FX & fees |

## 2. Production Repository & Project Structure

This repository follows a clean, domain-driven TypeScript/Node.js (v20 LTS) microservice layout designed to sit alongside Payme's existing Web/Mobile Core APIs:

```text
payme-whatsapp-orchestrator/
├── README.md                                  # Global architecture, setup, AI & 3rd-party dependency guide
├── package.json                               # Dependencies (express, bullmq, ioredis, pg, jose, pino, zod)
├── tsconfig.json                              # Strict TypeScript configuration
├── docker-compose.yml                         # Local PostgreSQL 16, Redis 7, & Orchestrator stack
├── .env.example                               # Template environment variables & secret keys
│
├── docs/                                      # Interactive Engineering Manuals & Diagrams
│   ├── payme_whatsapp_remittance_manual.html  # End-to-End WhatsApp Flow & Interactive Simulator Manual
│   ├── payme_server_tech.html                 # Internal Server Arch, Database ERD, SQL & 24 Conditions
│   └── flows/
│       └── payme_remittance_flow_v1.json      # WhatsApp Flow JSON v7.0 (SCREEN_QUOTE, BENEFICIARY, REVIEW)
│
├── migrations/                                # PostgreSQL 16 Database Migrations
│   ├── 001_create_payme_whatsapp_schema.sql   # New tables: wa_handoff_tokens, wa_conversations, wa_sessions...
│   └── 002_alter_existing_payme_core.sql      # Adds wa_session_id & channel='WHATSAPP' to core ledger
│
├── scripts/                                   # Operational & DevOps CLI Scripts
│   ├── generate-rsa-keys.sh                   # Generates 2048-bit RSA keypair & uploads public key to Meta
│   ├── sync-whatsapp-templates.ts             # Registers & syncs UTILITY templates with Meta Graph API
│   └── publish-whatsapp-flow.ts               # Uploads & publishes payme_remittance_flow_v1.json
│
├── src/
│   ├── index.ts                               # Express server bootstrap & graceful shutdown
│   ├── config/
│   │   ├── env.ts                             # Zod-validated environment configuration
│   │   ├── redis.ts                           # ioredis cluster client & key-space helpers
│   │   └── database.ts                        # PostgreSQL connection pool (pg)
│   │
│   ├── api/                                   # Tier 1: HTTP Ingress & Edge Controllers
│   │   ├── routes/
│   │   │   ├── whatsappWebhook.route.ts       # GET/POST /webhooks/whatsapp (Fast-Ack <100ms + HMAC check)
│   │   │   ├── whatsappFlows.route.ts         # POST /api/wa/flows-endpoint (RSA-OAEP + AES-GCM data_exchange)
│   │   │   ├── websiteHandoff.route.ts        # POST /v1/wa-handoff/init (Mints PMW-xxxxxx & wa.me link)
│   │   │   └── coreCallbacks.route.ts         # POST /webhooks/payme-core (kyc.approved, payment.captured)
│   │   └── middlewares/
│   │       ├── verifyMetaSignature.ts         # Validates X-Hub-Signature-256 over raw request buffer
│   │       └── redactPanGuardrail.ts          # Luhn/regex filter masking accidental card digits in chat
│   │
│   ├── workers/                               # Tier 2: Asynchronous BullMQ Worker Pool
│   │   ├── inboundState.worker.ts             # Acquires per-user mutex lock & runs State Machine transitions
│   │   ├── outboundDispatcher.worker.ts       # Checks 24h Meta window, sends JSON/Templates, handles 429 retry
│   │   └── sessionExpiry.worker.ts            # Marks abandoned >2h sessions & quotes as EXPIRED
│   │
│   ├── state-machine/                         # Deterministic Conversational State Machine (FSM)
│   │   ├── stepRouter.ts                      # Routes IDLE -> CORRIDOR -> AMOUNT -> MODE -> FLOW -> WEBVIEW
│   │   ├── reentryConflictHandler.ts          # Handles <2h website re-click (Resume vs. Start New)
│   │   └── conditionsValidator.ts             # Enforces all 24 runtime conditions (C-01 through C-24)
│   │
│   ├── ai/                                    # Optional AI & Multimodal Copilot Layer
│   │   ├── intentExtractor.service.ts         # LLM Structured Output parser for free-text & voice notes
│   │   ├── voiceTranscriber.service.ts        # Downloads WA audio media & transcribes via Whisper/ASR
│   │   ├── bankSlipOcr.service.ts             # Vision OCR extracting CLABE/IBAN/IFSC from user photos
│   │   ├── ragSupportCopilot.service.ts       # Corridor FAQ assistant grounded on Payme knowledge base
│   │   └── scamSignalDetector.service.ts      # Behavioural & coercion risk scoring before checkout
│   │
│   ├── integrations/                          # Tier 4: 3rd-Party & Payme Core Adapters
│   │   ├── meta/
│   │   │   ├── graphApiClient.ts              # POST /{PHONE_ID}/messages & media download client
│   │   │   └── flowsCrypto.ts                 # RSA-OAEP unwrap, AES-128-GCM decrypt, ~IV encrypt
│   │   ├── payme-core/
│   │   │   ├── fxQuote.client.ts              # locks 15-min FX rate & fee breakdown
│   │   │   ├── beneficiary.client.ts          # Bank account / CLABE validation & recipient CRUD
│   │   │   ├── kycSession.client.ts           # Mints signed WebView JWT for KYC provider (Sumsub/Onfido)
│   │   │   └── paymentCheckout.client.ts      # Mints signed WebView JWT for 3DS2 Card / ACH gateway
│   │   └── third-party/
│   │       ├── fxLiquidityProvider.adapter.ts # External wholesale FX feed adapter
│   │       ├── sanctionsAml.adapter.ts        # OFAC / PEP / Sanctions screening adapter
│   │       └── payoutNetwork.adapter.ts       # Corridor bank & cash-pickup aggregator adapter
│   │
│   └── templates/                             # Outbound WhatsApp Payload Builders
│       ├── interactivePayloads.ts             # List menus, Reply buttons, Flow launchers, CTA URL buttons
│       └── utilityTemplatePayloads.ts         # payme_transfer_receipt & payme_payout_status_update
│
└── tests/
    ├── unit/
    │   ├── flowsCrypto.test.ts                # Verifies RSA-OAEP + AES-GCM + flipped IV encryption
    │   └── conditionsValidator.test.ts        # Tests all 24 guardrails (limits, 2h TTL, PAN redaction)
    └── integration/
        └── fullLifecycleWebhook.test.ts       # Simulates website click -> Flow -> KYC/Payment -> Receipt
```

## 3. Third-Party Dependency Ecosystem & Architecture

A production WhatsApp remittance flow relies on **7 external 3rd-party provider categories** orchestrated behind Payme's backend. The Mermaid architecture diagram below illustrates every external dependency and how data flows between them:

```mermaid
flowchart TB
    subgraph Client["1. Customer Touchpoints"]
        Web["Payme Website / App\n(payme.com Calculator)"]
        WA["WhatsApp Mobile Client\n(Chat + Native Flows + In-App WebView)"]
    end

    subgraph Meta["2. Messaging & Form Platform (3rd Party)"]
        CloudAPI["Meta WhatsApp Cloud API v23.0\n(POST /{PHONE_ID}/messages & Webhooks)"]
        FlowsEngine["Meta WhatsApp Flows Engine\n(RSA-OAEP + AES-128-GCM Encrypted Proxy)"]
    end

    subgraph Payme["3. Payme Server Infrastructure"]
        Orch["Payme WhatsApp Orchestrator\n(Fast-Ack Ingress + BullMQ + 2h Redis FSM)"]
        Core["Payme Existing Core Backend\n(Users, Quotes, Beneficiaries, Ledger, WebView Host)"]
    end

    subgraph AI["4. AI & Multimodal Services (3rd Party - Optional)"]
        ASR["Speech-to-Text ASR\n(OpenAI Whisper / Deepgram)\nVoice Note Transcription"]
        LLM["LLM Intent & RAG Engine\n(Vertex AI Gemini / OpenAI)\nNLU & Corridor FAQ"]
        OCR["Vision OCR Engine\n(Gemini Vision / Document AI)\nBank Slip & IFSC/CLABE Extraction"]
    end

    subgraph FinProviders["5. Financial, Compliance & Payout 3rd-Party Providers"]
        FX["FX Rate & Liquidity Providers\n(Currencycloud / XE / Wise Platform)\nLive Spot Rates & 15m Rate Lock"]
        KYC["KYC, Biometrics & AML Screening\n(Sumsub / Onfido + ComplyAdvantage)\nID Liveness WebView & OFAC/PEP Check"]
        BankVal["Bank & Account Verification\n(Plaid / Local Penny-Drop / Banxico CEP)\nCLABE, IBAN, IFSC & Name Match"]
        PayIn["Payment Acquiring Gateway (Pay-In)\n(Stripe / Adyen / Checkout.com)\nPCI-DSS 3D Secure 2.0 Debit Card & ACH"]
        PayOut["Last-Mile Payout Aggregators (Pay-Out)\n(dLocal / Thunes / TerraPay / Uniteller)\nInstant Bank Deposit, Cash Pickup & Wallets"]
    end

    Web -->|"1. wa.me?text=Ref:PMW-9F3K82"| WA
    WA <-->|"2. Interactive JSON & Templates"| CloudAPI
    WA <-->|"3. Native Form Screens"| FlowsEngine
    CloudAPI <-->|"Webhooks & Outbound API"| Orch
    FlowsEngine <-->|"Encrypted data_exchange"| Orch
    Orch <-->|"Internal REST / Events"| Core

    Orch -.->|"Audio Media"| ASR
    Orch -.->|"Free-Text / FAQ"| LLM
    Orch -.->|"Check / Card Photo"| OCR

    Core <-->|"Spot Rate & Lock"| FX
    Core <-->|"ID Check & Sanctions"| KYC
    Core <-->|"Verify Account Name"| BankVal
    WA ===>|"4. Signed cta_url WebView"| Core
    Core <-->|"5. 3DS2 Charge / ACH"| PayIn
    Core <-->|"6. Disburse Local Currency"| PayOut
```

### Third-Party Dependency Specification Matrix

| Dependency Category | Role in Payme's WhatsApp Flow | Example Global Providers | Integration Protocol & SLA |
| :--- | :--- | :--- | :--- |
| **1. WhatsApp Business Platform** | Delivers interactive lists, reply buttons, native Flows, CTA URLs, and approved Utility Templates ([Meta Cloud API](https://developers.facebook.com/documentation/business-messaging/whatsapp/about-the-platform)) | Meta Platforms (Direct Cloud API) | HTTPS Graph API + HMAC Webhooks + RSA/AES Flow Encryption (`<8s` timeout) |
| **2. Wholesale FX & Treasury Feed** | Supplies real-time corridor mid-market rates and locks 15-minute customer quotes (`fx_quotes`) | Currencycloud, XE, OpenExchangeRates, Wise Platform | Streaming WebSocket / REST Polling (cached in Redis for `30s`, locked for `15m` per quote) |
| **3. KYC, Liveness & AML/Sanctions** | Verifies government ID + selfie liveness inside the signed WebView and screens sender + beneficiary against OFAC/PEP/Sanctions lists | Sumsub, Onfido, Jumio, Persona (KYC); ComplyAdvantage, LexisNexis (AML) | WebSDK embedded in Payme's KYC WebView (`verify.payme.com`) + Async Webhook (`kyc.approved`) |
| **4. Bank Account & Directory Validation** | Validates beneficiary bank account syntax (IBAN, 18-digit Mexican CLABE, Indian IFSC, Nepal bank code) and account holder name match during Flow submission | Local clearing house APIs (NPCI, Banxico, NCHL), Plaid, Modulr | Synchronous REST call (`<800ms`) inside `/api/wa/flows-endpoint` on `SCREEN_BENEFICIARY` |
| **5. Payment Acquiring Gateway (Pay-In)** | Processes Visa/Mastercard Debit Card payments with **3D Secure 2.0 (3DS2)** OTP, Apple Pay, Google Pay, or ACH inside the signed checkout WebView | [Stripe Payments](https://stripe.com/customers/felix), Adyen, Checkout.com, Worldpay | Hosted/Embedded PCI-DSS v4.0 Checkout (`pay.payme.com`) + Webhook (`payment.captured`) |
| **6. Last-Mile Payout Network (Pay-Out)** | Disburses local currency via instant bank rails (SPEI, UPI, IPS), mobile wallets (eSewa, Khalti, M-Pesa), or cash pickup networks (Elektra, OXXO, IME) | [dLocal](https://www.dlocal.com/press-releases/dlocal-and-felix-launch-instant-stablecoin-funded-whatsapp-remittances-across-latin-america/), Thunes, TerraPay, Nium, Uniteller | REST Disbursement API + Async Webhook (`payout.ready_for_pickup`, `payout.deposited`) |
| **7. AI, Speech & Vision APIs (Optional)** | Transcribes WhatsApp voice notes, extracts structured intent from free text, reads bank account photos via OCR, and answers corridor FAQs | Google Vertex AI (Gemini), OpenAI (Whisper / GPT-4o), Deepgram | REST / gRPC with strict JSON Schema output validation & deterministic fallback |

## 4. Where & How to Use AI in Payme's WhatsApp Flow

Leading conversational remittance players such as [Félix Pago](https://tempo.xyz/customer-stories/felix-pago/) and [Remitly](https://news.remitly.com/company-and-product-news/remitly-send-with-whatsapp/) combine **AI for natural input understanding** with **deterministic state machines for financial execution**.

> **Core Engineering Rule — "AI Suggests, Deterministic State Machine Commits":**
> Never allow an LLM to calculate exchange rates, invent transfer fees, bypass KYC gates, or trigger payment capture directly. The AI layer only **extracts structured parameters** or **answers support questions**, then hands control back to `wa_sessions` and Payme's core APIs.

### Four High-ROI AI Integration Points

| AI Use Case | Where It Fits in the Lifecycle | How It Works Technically | Deterministic Guardrail |
| :--- | :--- | :--- | :--- |
| **1. Multilingual Text & Voice-Note Intent Extraction** | **Stage 1–3 (Entry & Quote):** User sends a text or WhatsApp voice note like *"Quiero mandar 400 dólares a mi mamá María en México por BBVA"* or *"Send 500 USD to Ramesh in Nepal cash pickup"*. | 1. If `message.type === "audio"`, download media via Meta Graph API and transcribe with **Whisper / Deepgram**.<br>2. Pass transcript/text to **LLM with Structured Outputs** (`zod` schema: `{ intent, corridor_iso2, send_amount_usd, delivery_mode, beneficiary_name_hint }`). | Payme's state machine validates the extracted fields against `fx_quotes` and `beneficiaries`, locks the real rate in DB, and presents a 1-tap confirmation button to the user. |
| **2. Vision OCR for Bank Slips & ID Cards** | **Stage 5 (Beneficiary Creation):** Instead of typing an 18-digit CLABE or bank account number, the user sends a photo of a bank check, passbook, or account card in chat. | 1. Download `message.image.id` from Meta Graph API.<br>2. Run **Multimodal Vision OCR** to extract `{ bank_name, account_or_clabe, routing_ifsc, holder_name }`.<br>3. Pre-fill `SCREEN_BENEFICIARY` in the WhatsApp Flow via `flow_action_data`. | The user must visually review and tap **Confirm** inside the WhatsApp Flow screen, and Payme's Bank Validation API verifies the checksum/account before saving. |
| **3. RAG Corridor & Status Support Copilot** | **Any Stage (Interrupt & Resume):** Mid-transfer, the user asks *"How long does cash pickup take in Kathmandu?"* or *"What ID does my recipient need?"*. | When the user sends a question that doesn't match the expected step input, the **RAG Copilot** retrieves verified answers from Payme's Corridor Policy KB (and live `GET /remittances/{id}/status` if asking about a transfer). | Every AI answer appends a **Quick-Reply Resume Button** (e.g., `▶️ Continue $500 Transfer to Nepal`) so the user never drops out of their 2-hour session. |
| **4. Conversational Scam & Coercion Detection** | **Stage 6–8 (Pre-Checkout Risk Gate):** Detects romance scams, investment scams, or elder coercion before minting the payment link. | Scores session velocity, first-time high-risk corridor patterns, and urgent conversational signals in real time. | High-risk scores trigger an in-chat scam-warning disclosure or step-up verification in the KYC WebView. |

### AI-Augmented Webhook Routing Flow

```mermaid
sequenceDiagram
    autonumber
    participant User as User (WhatsApp)
    participant Meta as Meta Cloud API
    participant Orch as Payme Webhook Worker
    participant AI as AI Layer (Whisper / LLM / Vision)
    participant Core as Payme Core APIs (FX / Bene / DB)

    User->>Meta: Sends Voice Note or Text: "Send $500 to Ramesh in Nepal"
    Meta->>Orch: POST /webhooks/whatsapp (type: "audio" or "text")
    Orch->>AI: 1. Transcribe audio (ASR) & extract structured JSON intent
    AI-->>Orch: {intent: "SEND_MONEY", corridor: "NP", amountUsd: 500, beneHint: "Ramesh"}
    Orch->>Core: 2. Deterministic Lookup: Match "Ramesh" in saved beneficiaries & lock $500 NP Quote
    Core-->>Orch: {quoteId: "QT-991", payout: "67,400 NPR", beneId: "BEN-441", fee: "$1.99"}
    Orch->>Meta: 3. Update 2h Session (AWAITING_PAYMENT) & Send Interactive Confirmation CTA
    Meta->>User: Renders Pre-Filled Summary + "Pay $501.99 Securely" WebView Button
```

## 5. Database Schema & 2-Hour Session Lifecycle Summary

Full SQL DDL (`migrations/001_create_payme_whatsapp_schema.sql`), Crow's-Foot ERD, and the **24 Runtime Conditions Matrix (`C-01` to `C-24`)** are documented in `docs/payme_server_tech.html`.

| Table / Key | Storage Engine | TTL / Retention | Purpose |
| :--- | :--- | :--- | :--- |
| `payme:wa:handoff:{ref_id}` | Redis 7 + PG `wa_handoff_tokens` | **15 Minutes** (Single-use) | Bridges website click (`[Ref: PMW-9F3K82]`) to the user's WhatsApp phone number (`wa_phone`). |
| `payme:wa:session:{wa_phone}` | Redis 7 + PG `wa_sessions` | **2 Hours (`7200s`)** | Tracks `current_step`, `flow_token`, `quote_id`, `beneficiary_id`, `draft_tx_id`, and handles `<2h` website re-clicks (`Resume` vs. `Start New`). |
| `payme:wa:dedup:{wamid}` | Redis 7 | **24 Hours (`86400s`)** | Atomic `SETNX` lock preventing duplicate processing if Meta retries a webhook. |
| `payme:wa:lock:{wa_phone}` | Redis 7 | **5 Seconds (`5000ms`)** | Per-user mutex serializing rapid double-taps on interactive buttons. |
| `wa_conversations` | PostgreSQL 16 | Permanent | Tracks `service_window_expires_at` (`NOW() + 24h`), `user_id` link, and `last_paid_tx_id`. |
| `wa_message_logs` | PostgreSQL 16 | Permanent (Redacted) | Immutable audit log of all inbound/outbound payloads and Meta delivery statuses (`SENT`, `DELIVERED`, `READ`, `FAILED`). |
| `wa_template_registry` | PostgreSQL 16 | Permanent | Local cache of approved Meta Utility/Marketing templates (`payme_transfer_receipt`). |

## 6. Quick-Start Setup & Developer Commands

### 6.1 Environment Configuration (`.env.example`)

```bash
# Server & Database
NODE_ENV=production
PORT=8080
DATABASE_URL=postgresql://payme_app:secret@localhost:5432/payme_prod
REDIS_URL=redis://localhost:6379/0
SESSION_TTL_SECONDS=7200

# Meta WhatsApp Cloud API (v23.0+)
WHATSAPP_API_VERSION=v23.0
WABA_ID=546151681022936
WHATSAPP_PHONE_NUMBER_ID=102290129340398
PAYME_WA_DISPLAY_NUMBER=18005550199
WHATSAPP_SYSTEM_USER_TOKEN=EAAGm0PX4ZCpsBA...
WHATSAPP_APP_SECRET=8f92a1b4c3d2e1f0...
WHATSAPP_VERIFY_TOKEN=payme_webhook_verify_secret_2026

# WhatsApp Flows Encryption (2048-Bit RSA)
PAYME_FLOW_ID=1633450807827276
PAYME_FLOW_PRIVATE_KEY_PEM="-----BEGIN ENCRYPTED PRIVATE KEY-----\n..."
PAYME_FLOW_PRIVATE_KEY_PASSPHRASE=strong_passphrase_here

# Payme Core & WebView Security
PAYME_CORE_API_BASE_URL=https://internal-api.payme.com
PAYME_WEBVIEW_JWT_SECRET=hs256_webview_signing_secret_key
PAYME_KYC_WEBVIEW_URL=https://verify.payme.com/wa-kyc
PAYME_CHECKOUT_WEBVIEW_URL=https://pay.payme.com/wa/checkout

# Optional AI Copilot Layer
AI_COPILOT_ENABLED=true
GEMINI_API_KEY=AIzaSy...
OPENAI_API_KEY=sk-...
```

### 6.2 Installation, Database Migration & RSA Key Upload

```bash
# 1. Install dependencies & start local PostgreSQL 16 + Redis 7
npm install
docker compose up -d postgres redis

# 2. Run PostgreSQL schema migrations (creates new wa_* tables & alters core ledger)
psql "$DATABASE_URL" -f migrations/001_create_payme_whatsapp_schema.sql
psql "$DATABASE_URL" -f migrations/002_alter_existing_payme_core.sql

# 3. Generate 2048-bit RSA keypair & upload public key to Meta Cloud API for WhatsApp Flows
bash scripts/generate-rsa-keys.sh

# 4. Sync & verify APPROVED status of Utility Message Templates (payme_transfer_receipt)
npx tsx scripts/sync-whatsapp-templates.ts

# 5. Start the Express Ingress Server & BullMQ Workers
npm run build && npm start
```

## 7. Companion Engineering Documentation

* **End-to-End WhatsApp Remittance Flow & Interactive Simulator:** Open `docs/payme_whatsapp_remittance_manual.html` in any browser to test the 7-step interactive phone simulator, inspect live webhook/API JSON payloads, and review the sequence diagrams.
* **Internal Server Architecture, Database ERD & 24 Conditions Spec:** Open `docs/payme_server_tech.html` to inspect the 4-tier internal server diagram, Crow's-Foot ERD, SQL DDL, and filterable 24-condition guardrail matrix.
