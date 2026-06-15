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

COPY public.event_kinds (id, name, chain_id) FROM stdin;
1	ValueCreate	1
4	OrganizationAdd	1
5	ValidatorElect	1
22	OrderCancelled	1
10	CrownRewards	1
21	OrderCreated	1
14	ExecutionFailure	1
15	OrganizationRemove	1
16	AddressUnregister	1
17	AddressRegister	1
20	AddressMigration	1
23	OrderBid	1
25	PollCreated	1
26	PollVote	1
27	PollClosed	1
28	ValueUpdate	1
29	MasterClaim	1
30	Inflation	1
32	FileCreate	1
33	FileDelete	1
34	ValidatorRemove	1
35	Log	1
37	Unknown	1
38	ChainCreate	1
39	AddressLink	1
40	AddressUnlink	1
41	OrganizationCreate	1
42	OrderClosed	1
43	FeedCreate	1
44	FeedUpdate	1
45	ValidatorPropose	1
46	ValidatorSwitch	1
47	PackedNFT	1
48	ChannelCreate	1
49	ChannelRefill	1
50	ChannelSettle	1
51	LeaderboardCreate	1
52	LeaderboardInsert	1
53	LeaderboardReset	1
54	PlatformCreate	1
55	ChainSwap	1
56	ContractRegister	1
57	OwnerAdded	1
58	OwnerRemoved	1
59	DomainCreate	1
60	DomainDelete	1
61	TaskStart	1
62	TaskStop	1
63	Crowdsale	1
64	ContractKill	1
65	OrganizationKill	1
70	GasPayment	7
71	ValueCreate	7
72	OwnerRemoved	7
73	ValueUpdate	7
74	OrganizationCreate	7
76	OrganizationRemove	7
77	ChainSwap	7
78	CrownRewards	7
79	FileDelete	7
80	OrderCancelled	7
81	Log	7
82	OrderCreated	7
83	ContractUpgrade	7
84	Crowdsale	7
85	OrderBid	7
86	TokenBurn	7
87	PlatformCreate	7
88	AddressMigration	7
89	ContractDeploy	7
90	Infusion	7
91	OwnerAdded	7
92	Inflation	7
93	ValidatorElect	7
94	TokenMint	7
95	AddressUnregister	7
96	TokenStake	7
97	ValidatorRemove	7
98	TokenCreate	7
99	TokenSend	7
100	FileCreate	7
101	OrderFilled	7
102	TokenClaim	7
103	GasEscrow	7
104	TokenReceive	7
105	OrganizationAdd	7
106	AddressRegister	7
75	ContractKill	7
67	GovernanceSetGasConfig	1
3	TokenStake	1
66	Custom_V2	1
24	Custom	1
9723061	TokenSeriesCreate	1
68	GovernanceSetChainConfig	1
13	ContractUpgrade	1
36	ContractDeploy	1
19	OrderFilled	1
31	TokenCreate	1
18	Infusion	1
8	TokenClaim	1
7	GasPayment	1
6	GasEscrow	1
9	TokenBurn	1
2	TokenMint	1
69	SpecialResolution	1
11	TokenSend	1
12	TokenReceive	1
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
-- PostgreSQL database dump complete
--

\unrestrict XfoVkg1LQbOAwdapGNSGeYFKorFVuygIREfbsLI1eOtYiD0sBAR5OxfUJe0UDYe

