# Contributing

## Create an issue

[Create an issue](https://github.com/prebid/prebid-server/issues/new) describing the motivation for your changes.
Are you fixing a bug? Improving documentation? Optimizing some slow code?

Pull Requests without associated Issues may still be accepted, if the motivation is obvious.
However, this will help speed up code review if there's any uncertainty.

## Change the code

[Create a fork](https://help.github.com/articles/working-with-forks/) and make your code changes.
Fix code formatting and run the unit tests with:

```bash
./validate.sh
```

## Add tests

All pull requests must have 90% coverage in the changed code. Check the code coverage with:

```bash
./scripts/coverage.sh --html
```

Bugfixes should include a regression test which prevents that bug from being re-introduced in the future.

## Open a Pull Request

When you're ready, [submit a Pull Request](https://help.github.com/articles/creating-a-pull-request/)
against the `master` branch of [our GitHub repository](https://github.com/prebid/prebid-server/compare).

Pull Requests will be vetted through [Travis CI](https://travis-ci.com/).
To reproduce these same tests locally, do:

```bash
./validate.sh --nofmt --cov
```

If the tests pass locally, but fail on your PR, [update your fork](https://help.github.com/articles/syncing-a-fork/) with the latest code from `master`.

**Note**: We also have some [known intermittent failures](https://github.com/prebid/prebid-server/issues/103).
          If the tests still fail after pulling `master`, don't worry about it. We'll re-run them when we review your PR.
