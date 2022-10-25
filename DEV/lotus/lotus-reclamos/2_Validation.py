# Databricks notebook source
if (dbutils.fs.ls("/mnt/lotus-reclamos/")):
  if(dbutils.fs.ls("/mnt/lotus-reclamos-storage")):
    print("OK")