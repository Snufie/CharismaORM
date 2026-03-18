# Example ERD

This ERD reflects the schema style used in the repository examples.

```mermaid
erDiagram
  LOCATIE ||--o{ ROBOT : hosts
  ROBOT ||--o{ SENSOR : has
  SENSOR ||--o{ METING : emits
  ROBOT ||--o{ COMMANDO : receives
  GEBRUIKER ||--o{ COMMANDO : sends

  LOCATIE {
    uuid id PK
    string naam
    string adres
  }

  ROBOT {
    uuid id PK
    uuid locatieId FK
    string naam
    bool actief
    int batterijPercentage
  }

  SENSOR {
    uuid id PK
    uuid robotId FK
    string type
    string status
  }

  METING {
    uuid id PK
    uuid sensorId FK
    decimal waarde
  }

  COMMANDO {
    uuid id PK
    uuid robotId FK
    int verzondenDoorUserId FK
    string type
    string status
    json payload
    json response
  }

  GEBRUIKER {
    int id PK
    string naam
    string email
    string rol
  }
```

## Notes

- Exact names/types depend on your schema file.
- Relation cardinality and nullable behavior depend on `@relation(...)` and optional markers.
