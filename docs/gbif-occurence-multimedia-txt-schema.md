In a Darwin Core Archive (DwC-A) downloaded from GBIF, the relationship between `occurrence.txt` and `multimedia.txt` follows a **star schema** layout.

`occurrence.txt` acts as the **Core file** (the center of the star), while `multimedia.txt` acts as an **Extension file** linked back to the core.

---

### 1. The Linkage: How They Connect

The two files are mapped together using a single relational key: **`gbifID`**.

* **`occurrence.txt`**: Each row represents a unique biodiversity record and has a unique `gbifID`.
* **`multimedia.txt`**: May contain zero, one, or multiple rows for a single `gbifID` (since one physical herbarium sheet might have multiple photos taken of it, or close-ups of its label).

---

### 2. Schema for `occurrence.txt` (The Core Metadata)

This file contains the biological, geographical, and administrative data for the specimen. While it can feature over 200 columns of standardized Darwin Core terms, the most vital fields for a vascular plant herbaria project include:

| Column Name | Description | Example Data |
| --- | --- | --- |
| **`gbifID`** | The unique numerical primary key assigned by GBIF. | `402391023` |
| **`basisOfRecord`** | The physical nature of the record. For herbaria, this is always filtered to this value. | `PRESERVED_SPECIMEN` |
| **`scientificName`** | The full, three-part or two-part taxon name with authorship. | *Quercus alba L.* |
| **`taxonKey`** / **`speciesKey`** | Unique backbone taxonomic ID numbers used to group species regardless of spelling variations. | `2878688` |
| **`institutionCode`** / **`collectionCode`** | Identifiers for the home museum or herbarium hosting the physical asset. | `NY` (New York Botanical Garden) |
| **`catalogNumber`** | The barcode or physical filing number stamped on the sheet. | `NY00123456` |
| **`recordedBy`** | The name of the original collector who found the plant. | `Asa Gray` |
| **`eventDate`** | The ISO 8601 date the plant was harvested from the wild. | `1874-06-15` |
| **`decimalLatitude`** / **`decimalLongitude`** | GPS/Coordinate mapping of where the specimen originally grew. | `42.3601`, `-71.0589` |

---

### 3. Schema for `multimedia.txt` (The Asset Ledger)

This file is much narrower and strictly handles the digital representations of the specimen. It breaks down into media-specific fields:

| Column Name | Description | Example Data |
| --- | --- | --- |
| **`gbifID`** | The foreign key pointing straight back to `occurrence.txt`. | `402391023` |
| **`type`** | The type of media asset. For photos, this standard term is used. | `StillImage` |
| **`format`** | The MIME type indicating the file extension pattern. | `image/jpeg` or `image/tiff` |
| **`identifier`** | **The actual URL** where the high-resolution image asset is publicly hosted by the museum. | `https://sweetgum.nybg.org/images/v2/highres...jpg` |
| **`references`** | A web URL directing to the museum’s interactive webpage for that specimen. | `https://word.nybg.org/detail.php?irn=4920` |
| **`license`** | The text declaration or Creative Commons status of the photograph. | `CC BY 4.0` or `CC0` |
| **`creator`** / **`rightsHolder`** | The photographer or the legal institution holding the copyright to the image. | `The New York Botanical Garden` |

---

### Practical Data Layout Example

If a single white oak specimen (`gbifID: 101`) has a photo of the full sheet and a secondary close-up macro photo of its acorns, your files will structurally parse out like this:

**`occurrence.txt`**

```text
gbifID   scientificName   basisOfRecord       institutionCode
101      Quercus alba     PRESERVED_SPECIMEN  NY

```

**`multimedia.txt`**

```text
gbifID   type        format      identifier
101      StillImage  image/jpeg  https://museum.org/specimen101_full.jpg
101      StillImage  image/jpeg  https://museum.org/specimen101_acorn_zoom.jpg

```
