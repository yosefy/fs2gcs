# fs2gcs


for regular uploads/downloads don't provide "data" flag (only for tdime for now)

usage
```
./fs2gcs  -in /dir1/dir2/ -out gs://BUCKET/obj1/obj2/ -conc 128
```

it will coopy files like this:

`/dir1/dir2/dir3/file.txt -> gs://BUCKET/obj1/obj2/dir3/file.txt`




to upload TDIME:

```console
./fs2gcs  -in /some_dir/tdime_root_dir/ -out gs://BUCKET/something/tdime/ -conc 128 -data tdime
```

it will create

`gs://BUCKET/something/tdime/00`
`gs://BUCKET/something/tdime/01`

to download TDIME:
```console
./fs2gcs -in gs://index_v905/tdime_new/tdime_root_dir/ -out /mnt/sdb/test1/tdime/ -conc 128 -data tdime
```

it will create

`/mnt/sdb/test1/tdime//00`
`/mnt/sdb/test1/tdime//01`


