# Piperoni

Piperoni is a lightweight ETL framework for any data type, which allows you to make, track, and visualize atomic data transformations. Unlike some ETL tools, Piperoni relies on in-memory transformation, and thus is ideal for manipulating complex, diverse non-"big"-data.


Piperoni allows you to make and track atomic data transformations, ensures expected types are being passed from transformation to transformation, and allows you to easily see the state of the data at any point in time. Piperoni is a great tool for collaborative data pipelines, where visibility into data transformations is key.

# Getting Started

`piperoni` is a framework for ETL and data pipeline work. To get started, first install `piperoni`:

```bash
pip install git+ssh://git@github.com/CitrineInformatics/piperoni.git
```

# Documentation

Detailed instructions on installation and usage can be found in the complete [piperoni docs](https://citrineinformatics.github.io/piperoni/)

## Contributing

[The following best practices are required for contributing.](CONTRIBUTING.md)

In this repo, we follow PEP8 standards (using Black) and include Docstrings in all of work.

All functions should have unit testing.

## Best Practices

- Never use branching code in a `Pipeline` (e.g. if, else) without an explicit warning or failure. Particularly, do not use branching if the branches give rise to same or similar data.

- Do not use `deepcopy()` in any operators; this will cause unexpected behavior.

- Keep transforms atomic! This is the reason for Piperoni. Don't be lazy.

- Stuck? Piperoni logs every transformation! Just set it to debug mode!

- Have intermediate states be optionally output by using `Checkpoints`

- Do not use nestled Types when defining Types in your Operators (e.g. `Dict` **not** `Dict[str, str]`)

- Avoid hidden-states / adopt functional programming practices whenever possible

- Avoid multiple versions of files for optioning. Adopt argparse or similar instead whenever possible.

- Use named variables and either avoid or fill in optional variables in function calls.

- Do not hard code column names or similar, even when the function only ever applies to a single column or instance.

- Have a trusted reference. Always compare to trusted reference after changes to the pipeline. Update the reference as needed.

## Flagging Bugs and Requesting New Features

We funnel Bugs and Feature requests through Github issues. Create a new issue and select Bug Report or Feature Request (If you have neither a bug or feature request, open a regular issue). Add a concise title, fill in the template, and submit the issue.

## Citations

Example Band Gap data used in the example are from: Strehlow, W. H., & Cook, E. L. (1973). Compilation of energy band gaps in elemental and binary compound semiconductors and insulators. Journal of Physical and Chemical Reference Data, 2(1), 163-200.

