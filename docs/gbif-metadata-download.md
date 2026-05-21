# GBIF Metadata Download

To get a complete list of all entries for vascular plants that include herbaria image assets on [GBIF](https://www.gbif.org/), you need to filter by a specific taxonomic phylum, restrict the results to preserved specimens (herbaria), and ensure they contain image media.

Because this query will yield tens of millions of records, navigating them via the web interface is inefficient. The standard way to get this list is by initiating a filtered asynchronous download.

Here is how you can do it using both the website interface and the GBIF API:

### Method 1: Using the GBIF Web Interface

1. Go to the [GBIF Occurrence Search Page](https://www.gbif.org/occurrence/search).
2. Apply the following filters in the left-hand panel:
* **Scientific Name / Taxon**: Search for and select **`Tracheophyta`** (this is the phylum name for all vascular plants).
* **Basis of Record**: Select **`Preserved specimen`** (this restricts your search to herbarium sheets and physical collections rather than citizen science observations).
* **Media Type**: Select **`Image`** (this ensures every record has an attached digital photo asset).


3. Once the filters are applied, click the **Download** button at the top right of the search panel.
4. Choose the **Darwin Core Archive (DwC-A)** format. This format is ideal because it generates a `.zip` package containing:
* `occurrence.txt`: The main list of data entries.
* `multimedia.txt`: A ledger mapping the occurrences directly to their herbarium image URLs.



---

### Method 2: Programmatically via the GBIF API

If you want to automate the request or incorporate it into a script (using Python, R, or `curl`), you can send a `POST` request to the GBIF download API using the exact keys for your filters.

**Taxon Key for Tracheophyta:** `7707728`

#### Example API Request Payload

You can send a JSON object to `https://api.gbif.org/v1/occurrence/download/request` (requires your GBIF account credentials):

```json
{
  "creator": "your_gbif_username",
  "notificationAddresses": [
    "your_email@example.com"
  ],
  "sendNotification": true,
  "format": "DWCA",
  "predicate": {
    "type": "and",
    "predicates": [
      {
        "type": "equals",
        "key": "TAXON_KEY",
        "value": "7707728"
      },
      {
        "type": "equals",
        "key": "BASIS_OF_RECORD",
        "value": "PRESERVED_SPECIMEN"
      },
      {
        "type": "equals",
        "key": "MEDIA_TYPE",
        "value": "StillImage"
      }
    ]
  }
}

```

### Pro-Tip for Data Handling

Once your download is processed and unzipped, the `multimedia.txt` file will serve as your master list for image links, which you can link back to the metadata in `occurrence.txt` using the shared `gbifID` column. If you are using Python, you can also use the [`plantnet/gbif-dl`](https://www.google.com/search?q=%5Bhttps://github.com/plantnet/gbif-dl%5D(https://github.com/plantnet/gbif-dl)) library specifically designed to parse these queries and download the image assets efficiently.