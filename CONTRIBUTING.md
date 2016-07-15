# Contributing to Common Search

Thank you for your interest in contributing to [Common Search](https://about.commonsearch.org/)!

Use these links to get started:

* [Understand the project](#understand-the-project)
* [Bug Reports](#bug-reports)
* [Contributing to issues](#contributing-to-issues)
* [Local install](#local-install)
* [Pull Requests](#pull-requests)
* [Helpful links](#helpful-links)

As a reminder, all contributors are expected to follow the [Contributor Covenant](http://contributor-covenant.org/).


## Understand the project

Our backend has an [early documentation](https://about.commonsearch.org/developer/backend).

Other docs about our [general technical architecture](https://about.commonsearch.org/developer/architecture) are available too.


## Bug Reports

While testing our current [UI Demo](https://uidemo.commonsearch.org/) you should be able to find a few bugs (it's young!). Reporting them is a great first step to contributing!

First, locate the right repository for your issue:

- [cosr-front](https://github.com/commonsearch/cosr-front/issues) for frontend issues (layout, search behaviour, browser issues, ...)
- [cosr-results](https://github.com/commonsearch/cosr-results/issues) for issues related to the results themselves (relevance, title/description formatting, ...)

You can create issues in [cosr-back](https://github.com/commonsearch/cosr-back/issues) if you are pretty sure their root cause is in this repository. When in doubt, create the issue in [cosr-results](https://github.com/commonsearch/cosr-results/issues).

Before reporting the bug, make sure there isn't already an issue opened for that bug by looking at the open issues. When in doubt, **create the issue anyway** and a maintainer will help.

A great bug report should be precise, informative and courteous. Including direct links or screenshots of the issue is a big plus.


## Contributing to issues

All our [issues](https://github.com/commonsearch/cosr-back/issues) are tagged by difficulty, language and status. The label [help wanted](https://github.com/commonsearch/cosr-back/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22) indicates that you are welcome to start investigating the issue immediately!

If you are not sure yet on how to fix an issue, **don't worry**! Just post a comment in the issue saying that you are interested and a maintainer will help you. We really want to make it as easy as we can for newcomers and you will be welcomed with open arms.

We also add a [needs discussion](https://github.com/commonsearch/cosr-back/issues?q=is%3Aopen+is%3Aissue+label%3A%22needs+discussion%22) label to issues for which we are not yet sure of the right solution. We would love having your opinion on them!


## Local install

You should look at our [INSTALL.md](INSTALL.md) to learn how to setup this repository on you local machine. Spoiler: it's easy!


## Pull Requests

When contributing code, you will open a pull request so that the maintainers can check that everything looks good before merging your code. GitHub has a good documentation on [how pull requests work](https://help.github.com/articles/using-pull-requests/).

Before pushing your changes, you should try to run the tests locally with `make test`. [Travis-CI](https://travis-ci.org) will also automatically run them every time you push.

It's okay however to push failing tests in your branch if you need help from a maintainer!


## Helpful links

Here are some more places to look for information:

* [How to contribute](https://about.commonsearch.org/contributing): General info for developers, designers, testers, ...
* [cosr-participation](https://github.com/commonsearch/cosr-participation): Our repository dedicated to helping people get involved. You can create an issue there to give us feedback on your contribution experience!


Thanks again for joining the adventure!
