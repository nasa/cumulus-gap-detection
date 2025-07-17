# KnownGap API

API handler that supports adding and querying reasons to indicate a gap as 'known'

## Usage
**POST /knownGap** - Add reasons

Request:
```
{"reasons": [{"shortname": "M2T3NPRAD", "version": "5.12.4", "start_ts": "2000-01-01T00:00:00Z", "end_ts": "2000-12-31T23:59:59Z", "reason": "maintenance"}]}
```
201 Response:
```
{"message": "Sucessfully added reasons for: [{'shortname': 'M2T3NPRAD', 'version': '5.12.4', 'start_ts': '2000-01-01T00:00:00Z', 'end_ts': '2000-12-31T23:59:59Z', 'reason': 'maintenance'}]"}
```

**GET /knownGap** - Retrieve reasons 

Parameters: `short_name`, `version`, `startDate`, `endDate` (ISO datetime)

Example:
```
  {
    "short_name": "M2T3NPRAD",
    "version": "5.12.4",
    "startDate": "2000-01-01T00:00:00",
    "endDate": "2000-12-31T23:59:59"
  }
```

Returns:
```
{
  "reasons": [{"start_ts": "2000-01-01T00:00:00Z", "end_ts": "2000-12-31T23:59:59Z", "reason": "maintenance"}]
}
```
