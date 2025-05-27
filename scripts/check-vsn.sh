#!/bin/bash

set -euo pipefail

GIT_TAG=$(git describe --exact-match --tags HEAD 2>/dev/null || echo "")

erl -noshell -eval \
  "{ok, Conf} = file:consult(\"rebar.config\"),
  GitTag = \"$GIT_TAG\",
  {erl_opts, ErlOpts} = lists:keyfind(erl_opts, 1, Conf),
  {d, plugin_rel_vsn, Vsn1} = lists:keyfind(plugin_rel_vsn, 2, ErlOpts),
  {relx, RelxConf} = lists:keyfind(relx, 1, Conf),
  {release, {emqx_ecq, Vsn2}, _} = lists:keyfind(release, 1, RelxConf),
  case Vsn1 == Vsn2 of
    true ->
      ok;
    false ->
      io:format(\"version in erl_opts does not match release version, plugin_rel_vsn = ~s, but relx version = ~s\\n\", [Vsn1, Vsn2]),
      halt(1)
  end,
  case length(GitTag) > 0 andalso GitTag =/= Vsn2 of
    true ->
      io:format(\"git tag does not match release version, git tag = ~s, release version = ~s\\n\", [GitTag, Vsn2]),
      halt(1);
    false ->
      halt(0)
  end"
