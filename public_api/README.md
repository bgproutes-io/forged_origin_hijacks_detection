---

# Forged Origin Hijack Detection API

This API provides access to **DFOH** inference data. Users can query historical events using a variety of filters such as AS numbers, prefix criteria, RPKI validation status, and more.

---

## 🚀 API Endpoint

### `GET /forged_origin_hijacks`

Retrieve forged origin hijack events that match specific query filters.

#### ✅ Successful Response

Returns a list of hijack `Inference` objects.

Each object contains:

* `date`: Date of the hijack event (format: `YYYY-MM-DD`)
* `link`: AS link involved (`"asn1 asn2"`)
* `num_paths`: Number of distinct AS paths observed
* `classification`: `"leg"` or `"sus"` (legitimate vs. suspicious)
* `confidence`: Integer confidence score (e.g., 90)
* `new_links`: List of observations including:

  * `observed_at`: Timestamp (string)
  * `prefix`: Affected prefix (CIDR notation)
  * `as_path`: Full AS path
  * `vantage_point`: `"peer_asn peer_ip"`

---

## 🔍 Query Parameters

You can use one or more of the following query parameters:

| Parameter              | Type       | Description                                                                                                                 |
| ---------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------- |
| `asn`                  | string     | Comma-separated ASNs. Matches either end of the AS link. Max 10.                                                            |
| `attackers`            | string     | Comma-separated list of suspected attacker ASNs. Matches any involved. Max 10.                                              |
| `victims`              | string     | Comma-separated list of victim ASNs. Matches any involved. Max 10.                                                          |
| `classification`       | string     | One of: `leg`, `sus`. Filters by inferred legitimacy.                                                                       |
| `hijack_type`          | int        | Hijack type code (integer).                                                                                                 |
| `is_origin_rpki_valid` | bool (str) | `true` or `false`. Whether origin is RPKI-valid.                                                                            |
| `is_local`             | bool (str) | `true` or `false`. Whether hijack is a local case, i.e., observed by a VP in the attacker's AS (thus likely not a hijack)   |
| `is_recurrent`         | bool (str) | `true` or `false`. Whether new edge is reccurent                                                                            |
| `prefixes`             | string     | Comma-separated list of CIDR prefixes. Match behavior depends on `prefix_match_type`. Max 10.                               |
| `prefix_match_type`    | string     | One of: `exact`, `more_specific`, `less_specific`. Default is `exact`.                                                      |
| `peer_ips`             | string     | Comma-separated IPs of observation vantage points. Max 10.                                                                  |
| `peer_asns`            | string     | Comma-separated ASNs of peer vantage points. Max 10.                                                                        |
| `before_datetime`      | string     | Format: `YYYY-MM-DD`. Only include events observed **before** this date.                                                    |
| `after_datetime`       | string     | Format: `YYYY-MM-DD`. Only include events observed **after** this date.                                                     |
| `path_contains`        | string     | Comma-separated ASNs. Matches any AS that appears in the AS path. Max 10.                                                   |

**Example request:**

```http
GET /forged_origin_hijacks?asn=3356,174&classification=sus&is_origin_rpki_valid=false&after_datetime=2023-01-01
```

---

## 📦 Response Schema

```json
[
  {
    "date": "2023-06-15",
    "link": "2914 174",
    "num_paths": 2,
    "classification": "sus",
    "confidence": 8,
    "new_links": [
      {
        "observed_at": "2023-06-15 12:05:30",
        "prefix": "203.0.113.0/24",
        "as_path": "6939 2914 174",
        "vantage_point": "64500 192.0.2.1"
      },
      ...
    ]
  }
]
```

---

## 🧪 Notes

* Query lists (e.g., `asn`, `prefixes`) are limited to **10 items** for performance and safety.
* Prefix matching uses PostgreSQL's `inet` type operators: `=`, `<<=`, `>>=`.
* Invalid input raises 400 errors with detailed messages.
