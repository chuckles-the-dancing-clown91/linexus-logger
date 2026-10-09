#!/usr/bin/env bash
#
# Release helpers for .github/workflows/release.yml (release contract v1, see
# docs/operations/releases.md in daedalus-it). The same file lives in every
# product repository; keep the copies identical.
#
#   manifest.sh version [RUN_NUMBER]
#       YYYY.MM.DD.<run number> (UTC date). RUN_NUMBER defaults to
#       $GITHUB_RUN_NUMBER.
#   manifest.sh prev-tag
#       The nearest v* tag reachable from HEAD, or nothing on a first release.
#   manifest.sh notes [PREV_TAG]
#       The release body: `- <subject> (<short hash>)` for every commit since
#       PREV_TAG, or the last 20 commits when there is none.
#   manifest.sh changed PREV_TAG PATH...
#       Prints true when PATH changed between PREV_TAG and HEAD (always true
#       without a previous tag), false otherwise.
#   manifest.sh superseded EVENT
#       Exit 0 (and say why) when this commit must not be released: a v* tag
#       already points at a newer commit that contains it, or EVENT is "push"
#       and a v* tag already points at this very commit (a re-run). Exit 1 to
#       go ahead. A workflow_dispatch may re-release a released commit.
#   manifest.sh pack OUT.tar.gz STAGE_DIR
#       tar+gzip the contents of STAGE_DIR (at the archive root), with owner,
#       group, order and times normalised.
#   manifest.sh write DIR --product P --version V --commit SHA --repo OWNER/NAME
#                     [--notes-file F] [--migrations true|false] COMPONENT...
#       Writes DIR/release.json. COMPONENT is one of
#         binary:NAME:ASSET:OS:ARCH    (ASSET is a file in DIR)
#         web:NAME:ASSET               (ASSET is a file in DIR)
#         image:NAME:REF:DIGEST        (REF is image:tag, DIGEST sha256:<hex>)
#       "signed" is true exactly when RELEASE_SIGNING_KEY is set, which is
#       also when `sums` writes SHA256SUMS.sig.
#   manifest.sh sums DIR
#       Writes DIR/SHA256SUMS over every other file in DIR and, when
#       RELEASE_SIGNING_KEY holds an Ed25519 private key (PEM), DIR/SHA256SUMS.sig:
#       base64 (one line) of the raw signature over SHA256SUMS. The signature
#       is checked against the key's own public half before it is kept.
#
# Needs bash, git, jq, sha256sum, tar, gzip and, for signing, OpenSSL 3.

set -euo pipefail

die() { printf 'manifest.sh: %s\n' "$*" >&2; exit 1; }

cmd_version() {
  local run=${1:-${GITHUB_RUN_NUMBER:-}}
  [[ $run =~ ^[0-9]+$ ]] || die "version: need a run number (argument or GITHUB_RUN_NUMBER)"
  printf '%s.%s\n' "$(date -u +%Y.%m.%d)" "$run"
}

cmd_prev_tag() {
  git describe --tags --abbrev=0 --match 'v*' HEAD 2>/dev/null || true
}

cmd_notes() {
  local prev=${1:-}
  if [[ -n $prev ]]; then
    git log --format='- %s (%h)' "$prev..HEAD"
  else
    git log -n 20 --format='- %s (%h)' HEAD
  fi
}

cmd_changed() {
  local prev=${1:-}
  shift || true
  (($# > 0)) || die "changed: need at least one path"
  if [[ -z $prev ]]; then echo true; return; fi
  if git diff --quiet "$prev" HEAD -- "$@"; then echo false; else echo true; fi
}

cmd_superseded() {
  local event=${1:-} head t c
  head=$(git rev-parse HEAD)
  while IFS= read -r t; do
    [[ -n $t ]] || continue
    c=$(git rev-list -n 1 "$t")
    if [[ $c != "$head" ]]; then
      echo "release $t ($c) already contains $head; not releasing an older commit"
      return 0
    fi
    if [[ $event == push ]]; then
      echo "$head is already released as $t; nothing to do for a re-run"
      return 0
    fi
  done < <(git tag --list 'v*' --contains "$head")
  return 1
}

cmd_pack() {
  local out=${1:-} stage=${2:-}
  [[ -n $out && -d $stage ]] || die "pack: usage: pack OUT.tar.gz STAGE_DIR"
  case $out in /*) ;; *) out=$PWD/$out ;; esac
  # Every file gets the same time: the commit's, so a rebuild packs the same bytes.
  local epoch=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct HEAD 2>/dev/null || date +%s)}
  (cd "$stage" && tar --sort=name --owner=0 --group=0 --numeric-owner \
      --mtime="@$epoch" -cf - .) | gzip -n -9 >"$out"
}

sha_of() { sha256sum "$1" | cut -d' ' -f1; }

cmd_write() {
  local dir=${1:-}
  [[ -d $dir ]] || die "write: first argument must be the release directory"
  shift
  local product='' version='' commit='' repo='' notes_file='' migrations=false
  local comps='[]' spec kind name asset os arch ref digest rest
  while (($# > 0)); do
    case $1 in
      --product) product=$2; shift 2 ;;
      --version) version=$2; shift 2 ;;
      --commit) commit=$2; shift 2 ;;
      --repo) repo=$2; shift 2 ;;
      --notes-file) notes_file=$2; shift 2 ;;
      --migrations) migrations=$2; shift 2 ;;
      -*) die "write: unknown option $1" ;;
      *)
        spec=$1; shift
        kind=${spec%%:*}; rest=${spec#*:}
        case $kind in
          binary)
            IFS=: read -r name asset os arch <<<"$rest"
            [[ -n $name && -n $asset && -n $os && -n $arch ]] || die "write: bad component $spec"
            [[ -f $dir/$asset ]] || die "write: $dir/$asset does not exist"
            comps=$(jq -c --arg n "$name" --arg a "$asset" --arg s "$(sha_of "$dir/$asset")" --arg o "$os" --arg r "$arch" \
              '. + [{name:$n, kind:"binary", asset:$a, sha256:$s, os:$o, arch:$r}]' <<<"$comps")
            ;;
          web)
            IFS=: read -r name asset <<<"$rest"
            [[ -n $name && -n $asset ]] || die "write: bad component $spec"
            [[ -f $dir/$asset ]] || die "write: $dir/$asset does not exist"
            comps=$(jq -c --arg n "$name" --arg a "$asset" --arg s "$(sha_of "$dir/$asset")" \
              '. + [{name:$n, kind:"web", asset:$a, sha256:$s}]' <<<"$comps")
            ;;
          image)
            # REF itself contains a colon (repo:tag); the digest is the last two fields.
            name=${rest%%:*}; rest=${rest#*:}
            digest=${rest##*:}; rest=${rest%:*}
            digest="${rest##*:}:$digest"; ref=${rest%:*}
            [[ -n $name && -n $ref && $digest =~ ^sha256:[0-9a-f]{64}$ ]] || die "write: bad component $spec"
            comps=$(jq -c --arg n "$name" --arg i "$ref" --arg d "$digest" \
              '. + [{name:$n, kind:"image", image:$i, digest:$d}]' <<<"$comps")
            ;;
          *) die "write: unknown component kind in $spec" ;;
        esac
        ;;
    esac
  done
  [[ -n $product && -n $version && -n $repo ]] || die "write: --product, --version and --repo are required"
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || die "write: --commit must be a full 40-hex commit id"
  [[ $migrations == true || $migrations == false ]] || die "write: --migrations must be true or false"
  local notes=''
  if [[ -n $notes_file ]]; then notes=$(cat "$notes_file"); fi
  local signed=false
  [[ -n ${RELEASE_SIGNING_KEY:-} ]] && signed=true
  jq -n --arg product "$product" --arg version "$version" --arg commit "$commit" --arg repo "$repo" \
    --arg createdAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson signed "$signed" \
    --argjson migrations "$migrations" --argjson components "$comps" --arg notes "$notes" \
    '{schema:1, product:$product, version:$version, commit:$commit, repo:$repo,
      createdAt:$createdAt, signed:$signed, migrations:$migrations,
      components:$components, notes:$notes}' >"$dir/release.json"
}

cmd_sums() {
  local dir=${1:-}
  [[ -d $dir ]] || die "sums: usage: sums DIR"
  local files=() f
  rm -f "$dir/SHA256SUMS" "$dir/SHA256SUMS.sig"
  while IFS= read -r f; do files+=("$f"); done < <(cd "$dir" && find . -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
  ((${#files[@]} > 0)) || die "sums: $dir has no assets"
  (cd "$dir" && sha256sum -- "${files[@]}") >"$dir/SHA256SUMS.tmp"
  mv "$dir/SHA256SUMS.tmp" "$dir/SHA256SUMS"
  [[ -n ${RELEASE_SIGNING_KEY:-} ]] || { echo "RELEASE_SIGNING_KEY is not set: SHA256SUMS left unsigned" >&2; return 0; }

  local tmp
  tmp=$(umask 077 && mktemp -d)
  # shellcheck disable=SC2064  # expand now: tmp is local
  trap "rm -rf '$tmp'" EXIT
  printf '%s\n' "$RELEASE_SIGNING_KEY" >"$tmp/key.pem"
  openssl pkey -in "$tmp/key.pem" -pubout -out "$tmp/pub.pem" 2>/dev/null ||
    die "sums: RELEASE_SIGNING_KEY is not a private key openssl can read"
  openssl pkeyutl -sign -rawin -inkey "$tmp/key.pem" -in "$dir/SHA256SUMS" | base64 -w0 >"$tmp/sig"
  base64 -d "$tmp/sig" >"$tmp/sig.bin"
  openssl pkeyutl -verify -pubin -inkey "$tmp/pub.pem" -rawin -in "$dir/SHA256SUMS" -sigfile "$tmp/sig.bin" >/dev/null ||
    die "sums: the signature does not verify against the key's own public half (is it an Ed25519 key?)"
  mv "$tmp/sig" "$dir/SHA256SUMS.sig"
  echo "signed SHA256SUMS (public key: $(openssl pkey -pubin -in "$tmp/pub.pem" -outform DER | sha256sum | cut -c1-16)…)" >&2
}

sub=${1:-}
shift || true
case $sub in
  version) cmd_version "$@" ;;
  prev-tag) cmd_prev_tag ;;
  notes) cmd_notes "$@" ;;
  changed) cmd_changed "$@" ;;
  superseded) cmd_superseded "$@" ;;
  pack) cmd_pack "$@" ;;
  write) cmd_write "$@" ;;
  sums) cmd_sums "$@" ;;
  *) awk 'NR > 2 { if (!/^#/) exit; sub(/^# ?/, ""); print }' "$0" >&2; exit 2 ;;
esac
