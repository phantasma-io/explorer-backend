--
-- PostgreSQL database dump
--

\restrict XfoVkg1LQbOAwdapGNSGeYFKorFVuygIREfbsLI1eOtYiD0sBAR5OxfUJe0UDYe

-- Dumped from database version 17.4
-- Dumped by pg_dump version 17.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: chains; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.chains (id, name, current_height) FROM stdin;
7	main-generation-1	410367
1	main	8789419
\.


--
-- Data for Name: event_kinds; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.event_kinds (id, name) FROM stdin;
1	ValueCreate
4	OrganizationAdd
5	ValidatorElect
22	OrderCancelled
10	CrownRewards
21	OrderCreated
14	ExecutionFailure
15	OrganizationRemove
16	AddressUnregister
17	AddressRegister
20	AddressMigration
23	OrderBid
25	PollCreated
26	PollVote
27	PollClosed
28	ValueUpdate
29	MasterClaim
30	Inflation
32	FileCreate
33	FileDelete
34	ValidatorRemove
35	Log
37	Unknown
38	ChainCreate
39	AddressLink
40	AddressUnlink
41	OrganizationCreate
42	OrderClosed
43	FeedCreate
44	FeedUpdate
45	ValidatorPropose
46	ValidatorSwitch
47	PackedNFT
48	ChannelCreate
49	ChannelRefill
50	ChannelSettle
51	LeaderboardCreate
52	LeaderboardInsert
53	LeaderboardReset
54	PlatformCreate
55	ChainSwap
56	ContractRegister
57	OwnerAdded
58	OwnerRemoved
59	DomainCreate
60	DomainDelete
61	TaskStart
62	TaskStop
63	Crowdsale
64	ContractKill
65	OrganizationKill
70	GasPayment
83	ContractUpgrade
86	TokenBurn
89	ContractDeploy
90	Infusion
94	TokenMint
96	TokenStake
98	TokenCreate
99	TokenSend
101	OrderFilled
102	TokenClaim
103	GasEscrow
104	TokenReceive
67	GovernanceSetGasConfig
66	Custom_V2
24	Custom
9723061	TokenSeriesCreate
68	GovernanceSetChainConfig
69	SpecialResolution
\.


--
-- Data for Name: signature_kinds; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.signature_kinds (id, name) FROM stdin;
1	Ed25519
\.


--
-- Data for Name: transaction_states; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.transaction_states (id, name) FROM stdin;
1	Halt
2	Fault
3	Break
\.


--
-- Data for Name: organizations; Type: TABLE DATA; Schema: public; Owner: -
-- The two stake organizations (values as stored on mainnet), so
-- reconcile_stake_memberships is exercisable against the test database.
--

COPY public.organizations (id, organization_id, name, create_event_id, address, address_name) FROM stdin;
2	masters	masters	\N	S3dH4Ek14E5wWXvfmae6Wb4MHAmpGV36TnLE79V9MNod79V	masters
3	stakers	stakers	\N	S3dBJmaik2r9CoKSQeU5NE6Mjk7UCjbSBzedNCK6kNSzgqS	stakers
\.


--
-- Name: chains_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.chains_id_seq', 7, true);


--
-- Name: event_kinds_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.event_kinds_id_seq', 11413451, true);


--
-- Name: signature_kinds_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.signature_kinds_id_seq', 1, true);


--
-- Name: transaction_states_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.transaction_states_id_seq', 3, true);


--
-- Name: organizations_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.organizations_id_seq', 3, true);


--
-- PostgreSQL database dump complete
--

\unrestrict XfoVkg1LQbOAwdapGNSGeYFKorFVuygIREfbsLI1eOtYiD0sBAR5OxfUJe0UDYe

