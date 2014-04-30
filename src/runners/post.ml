(*
  Compile using ocamlc post.ml -o post
  run using ./plot <selected> <stats>
  The file <selected> a list of column headers separated by new lines.
  The file <stats> is the output of cvc4 --stats.
  The result is a comma seperated file over just the statistics names in selected.
  If a column name is not in <stats>, the value of the column will be "NA".

  For example, suppose <selected> was:
    sat::conflicts
    sat::propagations
    notthere

  and <stats> was:
    sat::clauses_literals, 0
    sat::conflicts, 1
    sat::decisions, 0
    sat::learnts_literals, 0
    sat::max_literals, 0
    sat::propagations, 11

  The output will be:
    1, 11, NA
*)

open Printf
open List
open String
module MS = Map.Make(String);;

let true_func x = true;;

let na = "NA";;

let not_excluded x =
  match x with
    "sat"
  |"unsat"
  |"valid"
  |"invalid"
  |"unknown" -> false;
  | _ -> true;;


let rec read_lines ic filter lines =
  try
    let line = input_line ic in
    if filter line
    then read_lines ic filter (line::lines)
    else read_lines ic filter lines
  with
    End_of_file -> lines;;

let filter_file filter argname =
  let ic = open_in argname in
  try
    let lines = read_lines ic filter [] in
    close_in ic;
    rev lines
  with e ->
    close_in_noerr ic;
    raise e;;

let split_stat line =
  let p = index line ',' in (* let the exception be thrown *)
  let l = length line in
  (* 0 ...  p-1, ','@p, p+1, ..., l-1 *)
  (* l-1 - (p+1) + 1 = l - p - 1 *)
  let fhalf = sub line 0 p in
  let shalf = sub line (p+1) (l - 1 - p) in
  fhalf, shalf;;

let split_stats stat_lines =
  let fl m line =
    let (k,v) = split_stat line in
    MS.add k v m
  in
  fold_left fl MS.empty stat_lines;;

let find_fail k m fail =
  try
    MS.find k m
  with Not_found->
    fail;;


let print_list m l =
  let rec pl m l pre =
    match l with
      [] -> Printf.printf "\n"
    | k::t ->
      let v = find_fail k m na in
      Printf.printf "%s %s" pre v;
      pl m t ","
  in
  pl m l "";;


let () =
  let selected_fname = Sys.argv.(1) in
  let raw_fname = Sys.argv.(2) in
  let selected = filter_file true_func argname in
  let raw = filter_file not_excluded inputname in
  let stat_map = split_stats raw in
  print_list stat_map selected;
  exit 0;;

