# uncompyle6 version 3.2.6
# Python bytecode 3.7 (3394)
# Decompiled from: Python 3.7.2 (default, Dec 29 2018, 06:19:36) 
# [GCC 7.3.0]
# Embedded file name: /home/ben_coolship_io/dd-alignment-server/watchers/xumi_files.py
# Size of source mod 2**32: 1202 bytes
import csv
coord_cols = [
 [
  'id', id],
 [
  'x', lambda x: round(float(x), 4)],
 [
  'y', lambda y: round(float(y), 4)]]
intfun = lambda x: int(float(x))
annotation_cols = [
 [
  'id', intfun],
 [
  'type', intfun],
 [
  'ar', intfun],
 [
  'tr', intfun],
 [
  'seq', str]]

seg_cols = [['id', int],
 [
  'seg', lambda x: int(float(x))],
 [
  'unk1', lambda x: None],
 [
  'unk2', lambda x: None],
 [
  'unk3', lambda x: None],
 [
  'unk4', lambda x: None],
 [
  'unk5', lambda x: None],
  [
  'unk6', lambda x: None],]

def parse_coords_file(filename):
    return [[coord_cols[i][1](e) for i, e in enumerate(l)] for l in csv.reader(open(filename))]


def parse_annotation_file(filename):
    print(filename)
    print(annotation_cols)
    return [[annotation_cols[i][1](e) for i, e in enumerate(l)] for l in csv.reader(open(filename))]


def parse_segmentation_file(filename):
    return [[seg_cols[i][1](e) for i, e in enumerate(l)] for l in csv.reader(open(filename))]
# okay decompiling watchers/__pycache__/xumi_files.cpython-37.pyc
