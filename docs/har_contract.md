# API Contract from HAR

## Endpoints
- `PreInscription/GetPreInscription` (GET)
- `RendezVous/Create` (OPTIONS, POST)
- `RendezVous/GetAvailableDates` (GET)
- `RendezVous/GetRendezVousInfosForPut` (GET)
- `RendezVous/Put` (OPTIONS, PUT)
- `download/HonneurEngagementReport` (GET)
- `download/RdvReport` (GET)
- `validateCandidate/query` (GET)

## Required Headers (per endpoint)
- `PreInscription/GetPreInscription`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `RendezVous/Create`: Accept, Accept-Encoding, Accept-Language, Access-Control-Request-Headers, Access-Control-Request-Method, Cache-Control, Connection, Content-Length, Content-Type, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, g-recaptcha-response, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `RendezVous/GetAvailableDates`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `RendezVous/GetRendezVousInfosForPut`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `RendezVous/Put`: Accept, Accept-Encoding, Accept-Language, Access-Control-Request-Headers, Access-Control-Request-Method, Cache-Control, Connection, Content-Length, Content-Type, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, g-recaptcha-response, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `download/HonneurEngagementReport`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `download/RdvReport`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
- `validateCandidate/query`: Accept, Accept-Encoding, Accept-Language, Cache-Control, Connection, Host, Origin, Pragma, Referer, Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, User-Agent, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform
