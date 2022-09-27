# FreSh

Repository for the paper "FreSh: Lock-Free Index for Sequence Similarity Search", Panagiota Fatourou, Eleftherios Kosmas, Themis Palpanas, George Paterakis. Under review, VLDB 2022.

# General

Odyssey is the the first lock-free (thus, highly fault-tolerant) data series index that exhibits the same performance as the state-of-the-art lock-based in-memory indexes.

# How to compile

To compile FreSh, use:

```
cd ads/

autoreconf
chmod u+x configure
./configure

automake
make
```


# Before running

Create required folders

```
mkdir results
mkdir perf
```

Install jemalloc (for better performance) following the instructions here: 'https://github.com/jemalloc/jemalloc/blob/dev/INSTALL.md'

# Parameters

The main parameters of FreSh are listed below:

`--timeseries-size [<int>]` (default: 256)

`--dataset [<string>]`

`--dataset-size [<int>]`       

`--queries [<string>]`

`--queries-size [<int>]`

`--leaf-size [<int>]`

`--initial-lbl-size`

`--min-leaf-size`

`--flush-limit`

`--in-memory`

`--cpu-type [<int>]`: defines the number of threads

`--function-type [<int>]`: defines the algorithm to run

`--ts-group-length [<int>]`

`--backoff-power [<int>]`: setting it to `-1` disables backoff

# Examples

```
./bin/ads --dataset "/spare/ekosmas/Datasets/Random/dataset100GB.bin" --dataset-size "104857600" --queries "/spare/ekosmas/Datasets/Random/query100.bin" --queries-size "100" --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --flush-limit 1000000 --in-memory --cpu-type 80 --function-type 999777013  --ts-group-length 512 --backoff-power -1

./bin/ads --dataset "/spare/ekosmas/Datasets/Sift/sift_len_128.bin" --dataset-size "104857600" --queries "/spare/ekosmas/Datasets/Sift/queries_size100_sift.bin" --queries-size "100" --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000  --flush-limit 1000000 --in-memory --cpu-type 40 --function-type 999777013 --ts-group-length 512 --backoff-power 100000 --timeseries-size 128
```

# People Involved

Panagiota Fatourou, Professor, University of Crete, ICS-FORTH, University of Paris and LIPADE (faturu@ics.forth.gr)

Eleftherios Kosmas, Postdoctoral Researcher, University of Crete (ekosmas@csd.uoc.gr) and ICS-FORTH

Themis Palpanas, Professor, University of Paris and LIPADE (themis@mi.parisdescartes.fr)

George Paterakis, Student, University of Crete and ICS-FORTH (geopat@ics.forth.gr)

# License

This code is provided under the [LGPL-2.1 License](https://github.com/PLATON-MARIE-CURIE-PROJECT/FreSh/blob/main/LICENSE)

# FreSh

FreSh is developed under the Platon research project.
