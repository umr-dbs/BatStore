# cMVBT-OSIC
> Original MVBT and cMVBT Papers:
```bibtex
@article{becker1996asymptotically,
  title={An asymptotically optimal multiversion B-tree},
  author={Becker, Bruno and Gschwind, Stephan and Ohler, Thomas and Seeger, Bernhard and Widmayer, Peter},
  journal={The VLDB Journal},
  volume={5},
  number={4},
  pages={264--275},
  year={1996},
  publisher={Springer}
}
@article{tonta2026multiversion,
  title={Multiversion Concurrency Control for Multiversion B-Trees},
  author={Tonta, Amir and Seeger, Bernhard and Soisalon-Soininen, Eljas},
  journal={arXiv preprint arXiv:2606.09133},
  year={2026}
}
```
> Ordered Snapshot Instant Commit from Paper (LeanStore):
```bibtex
@article{alhomssi2023scalable,
  title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
  author    = {Alhomssi, Adnan and Leis, Viktor},
  journal   = {Proceedings of the VLDB Endowment},
  volume    = {16},
  number    = {6},
  pages     = {1426--1438},
  year      = {2023},
  publisher = {VLDB Endowment},
  doi       = {10.14778/3583140.3583157}
}
```
---------------------------------------

CROSS-ENGINE BENCHMARK HARNESS - MANUAL
========================================
    Read manual.txt

## Engineering notes

- [Range-scan iteration: ordered routing and zero-copy streaming](docs/range_scan_iteration.md)
- [Range-scan visibility-check optimization](docs/range_scan_visibility_check.md)
- [Big-tree leaf-size benchmark](docs/bigtree_size_benchmark.md)
- [OLTP/WAL optimization](docs/oltp_wal_optimization.md)

# Contact
    Name:               Amir Tonta
    E-Mail:             amir.tonta@mathematik.uni-marburg.de
