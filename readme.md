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

- **[Transactional Support via OSIC on cMVBT: System Design, Datastructure Changes, and Optimizations](docs/transactional_osic_comprehensive.tex)** ([PDF](docs/transactional_osic_comprehensive.pdf)) --
  comprehensive synthesis of system design, all datastructure modifications, and every optimization tested or applied, with measured performance numbers and adoption decisions.

- [Unified optimization report](docs/optimization_report.tex) ([PDF](docs/optimization_report.pdf)) --
  the implementation's optimizations organized bottom-up by architectural dependency, with fresh measurements and known open issues.

- [Index optimization guide](docs/index_optimizations.md) -- compact guide to the optimizations used by the current cMVBT index

- Supporting documentation:
  - [Range-scan iteration: ordered routing and zero-copy streaming](docs/range_scan_iteration.md)
  - [Range-scan visibility-check optimization](docs/range_scan_visibility_check.md)
  - [Big-tree leaf-size benchmark](docs/bigtree_size_benchmark.md)
  - [OLTP/WAL optimization](docs/oltp_wal_optimization.md)
  - [DataFusion SQL integration and usage](docs/datafusion_sql.md)

# Contact
    Name:               Amir Tonta
    E-Mail:             amir.tonta@mathematik.uni-marburg.de
