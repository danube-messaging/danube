# Releasing Danube

How to cut a new Danube release. Follow the steps in order.

The user decides, the agent proposes. Stop and wait for the user's answer at each of these points:

- **The version** (step 0): propose one, the user picks.
- **Pushing to `main`** (steps 2 and 5).
- **Publishing to crates.io** (step 3): it cannot be undone.
- **The CHANGELOG section** (step 4): show the full section, the user reviews it before it is committed.
- **Tagging** (step 6): it starts the public release.

```bash
# Example: release v0.17.1
sed -i 's/0\.17\.0/0.17.1/' */Cargo.toml          # 1. bump versions (check the diff)
git commit -am "prepare release v0.17.1"           # 2. commit (cargo publish needs it)
git push origin main                               #    and usually push
(cd danube-core && cargo publish)                  # 3. publish core first
(cd danube-client && cargo publish)                #    then the client
./scripts/prepare-changelog.sh v0.17.1             # 4. generate, curate, user reviews, commit
git push origin main                               # 5. push
git tag v0.17.1 && git push origin v0.17.1         # 6. tag: starts release.yml
```

## Choosing the version

The current version is in any crate's `Cargo.toml` (`version = "X.Y.Z"`). The last release tag is `git tag --sort=-v:refname | head -1`.

- **Patch release** (bug fixes, docs, small improvements): `0.17.0` → `0.17.1`.
- **Feature release** (new features, breaking changes): `0.17.0` → `0.18.0`.

The version is the user's decision. Propose one with a one-line reason based on the commits since the last tag (step 0), and wait for the answer before changing any file.

## Step 0: preflight

```bash
git checkout main && git pull
git status --short                 # must be empty
git log --oneline $(git tag --sort=-v:refname | head -1)..HEAD   # what goes into the release
```

- Run the test tiers from [`testing.md`](testing.md): Tier 1 always, Tier 2 for any code change since the last tag.
- Do not start a release on a red `main`.
- Propose the version (see above) and wait for the user to confirm it.

## Step 1: bump the version in every crate

Every crate carries the version on line 3 of its `Cargo.toml`:

- `danube-core`, `danube-client`, `danube-schema`, `danube-raft`, `danube-persistent-storage`
- `danube-edge`, `danube-iceberg`, `danube-admin`, `danube-broker`

`danube-client` also pins the `danube-core` version it depends on. Change it to the new version too:

```toml
# danube-client/Cargo.toml
danube-core = {path = "../danube-core", version = "0.17.1"}
```

Check that nothing was missed or changed by accident:

```bash
grep -rn --include=Cargo.toml '0\.17\.0' .        # old version: no hits left
grep -rn --include=Cargo.toml '0\.17\.1' .        # new version: 10 hits (9 crates + the client pin)
cargo build                                        # refreshes the local Cargo.lock
```

Other files that mention a version (`README.md`, `agent_bench/`) refer to the agent bench report of a past release. Leave them alone (see "Not part of the release yet" at the end).

## Step 2: commit and push the version bump

```bash
git add */Cargo.toml
git commit -m "prepare release v0.17.1"
git push origin main               # ask first
```

`cargo publish` refuses to run while a crate's `Cargo.toml` has uncommitted changes, so the bump is committed before publishing. The user usually pushes it at this point too; ask before pushing.

## Step 3: publish the crates to crates.io

Only `danube-core` and `danube-client` are published. Core goes first because the client depends on the published version of core.

```bash
cd danube-core
cargo publish --dry-run            # packages and builds, uploads nothing
cargo publish
cd ../danube-client
cargo publish --dry-run
cargo publish
cd ..
```

- **Publishing cannot be undone.** A version can only be yanked, never replaced. Ask before running `cargo publish`.
- If the client's dry run cannot find the new `danube-core`, crates.io is still indexing it. Wait a minute and retry.
- Check both on crates.io: `https://crates.io/crates/danube-core` and `https://crates.io/crates/danube-client`.

## Step 4: write the CHANGELOG entry

1. Generate the raw entry from the commits since the last tag:

   ```bash
   ./scripts/prepare-changelog.sh v0.17.1
   ```

   It adds a section to the top of `CHANGELOG.md` with a `### What's Changed` list of commits, between these markers:

   ```markdown
   <!-- v0.17.1 START -->
   ## v0.17.1 - 2026-12-01
   ### What's Changed
   * ...
   <!-- v0.17.1 END -->
   ```

2. **Keep the markers exactly as generated.** `release.yml` copies everything between `<!-- vX.Y.Z START -->` and `<!-- vX.Y.Z END -->` into the GitHub release notes, and fails the release if the block is missing or empty. The tag must match the markers.

3. If there is an `## Unreleased` section (written during development), delete it. Use what it says as input for the curated highlights in the new section, rewritten for this release. Nothing may stay above the new `START` marker.

4. Curate the section for a public audience. Readers are users who do not know Danube's internals:

   - Open with one or two sentences on what the release means for users.
   - Group the highlights under short `###` headings with an emoji (see earlier entries in `CHANGELOG.md`), most important first.
   - Describe effects, not implementation: "Restarts resume where the node left off", not "persist openraft snapshots in redb".
   - Leave out internal refactors, CI, test-only and docs-only changes, unless they matter to users.
   - Add a short command example when a feature is easiest to show.
   - Keep the generated `### What's Changed` commit list at the end of the section.
   - Follow the writing style in `AGENTS.md`: short sentences, lists, no em dashes.

5. Show the user the complete new section, from `<!-- v0.17.1 START -->` to `<!-- v0.17.1 END -->`, and wait for approval. Apply their changes and show it again until they approve. Do not commit before that.

6. Commit:

   ```bash
   git add CHANGELOG.md
   git commit -m "changelog for v0.17.1"
   ```

## Step 5: push to main

Ask first, then:

```bash
git push origin main
```

Confirm the push went through: `git status` shows the branch is up to date with `origin/main`.

## Step 6: tag the release

Ask first: the tag starts the public release. Then:

```bash
git tag v0.17.1
git push origin v0.17.1
```

Pushing a `vX.Y.Z` tag starts [`release.yml`](../.github/workflows/release.yml), which:

- builds `danube` for Linux x86_64, macOS Apple Silicon and Windows x86_64, with the `lakehouse` feature;
- builds and pushes the multi-arch Docker image `ghcr.io/danube-messaging/danube:vX.Y.Z` (and `:latest`);
- creates the GitHub release with the archives, `checksums.txt` and the notes from `CHANGELOG.md`.

Do not wait for the run to finish: it takes a while, and the user checks it separately. Go straight to step 7 once the tag is pushed.

If the user later reports a failed run, do not delete or move the tag on your own. Look at the failing job and its log, and ask.

## Step 7: report the result

Report right after pushing the tag, using this template. Fill in every line; write "not done" or "failed" with the reason instead of leaving a line out. The release CI is not part of this report: give the links so the user can follow it.

```markdown
## Release vX.Y.Z

- **Version:** vA.B.C → vX.Y.Z (patch | feature)
- **Commits:** <bump commit sha>, <changelog commit sha> on main (pushed: yes/no)
- **Tests before release:** <tiers run and result, from docs/testing.md>
- **crates.io:** danube-core X.Y.Z <published / failed>, danube-client X.Y.Z <published / failed>
- **Tag:** vX.Y.Z pushed <yes/no>
- **Release CI:** started, follow it at https://github.com/danube-messaging/danube/actions/workflows/release.yml
- **When CI is done:** release page https://github.com/danube-messaging/danube/releases/tag/vX.Y.Z (3 archives + checksums.txt) and image ghcr.io/danube-messaging/danube:vX.Y.Z
- **Highlights:** <2 or 3 bullets from the CHANGELOG>
- **Follow-ups:** <anything left to do, or "none">
```

## Not part of the release yet

Leave these alone for now. They may become release steps later:

- Updating version references in the `danube-docs` repo.
- Running the [`agent_bench`](../agent_bench/AGENTS.md) suite and publishing a new report for the release.
