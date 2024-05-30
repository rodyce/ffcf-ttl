# ffcf-ttl
Test TTL delete functionality with All Versions and Deletes Change Feed

To use, first ingest data using the **ingest** paramater as follows:

```powershell
PS> dotnet run ingest
```

Then, wait for a couple of minutes while documents autmatically expire. They should later be seen with All Versions and Deletes Change Feed. Read the change feed with the **read** parameter as follows

```powershell
PS> dotnet run read
```

You should see creation and deletion event lines like these:

```text
Operation: create. Item id: 37ac68c8-9281-412f-bd4d-d38009715971_49. Current value: 70205
Operation: create. Item id: 4ac329b1-ab79-4916-b097-8fa865f95406_50. Current value: 3088
100 Operation: delete (due to TTL). Item id: a3d3087a-9473-4eea-9455-938b79b653d0_1. Previous value: 16443
Operation: delete (due to TTL). Item id: 8aa4299d-3268-4d8c-a07e-2b498961579f_2. Previous value: 81232
Operation: delete (due to TTL). Item id: 354efa6e-17e3-4a30-a6e5-c874fb766b8b_3. Previous value: 49046
```
