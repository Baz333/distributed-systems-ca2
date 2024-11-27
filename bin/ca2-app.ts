#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { CA2AppStack } from "../lib/ca2-app-stack";

const app = new cdk.App();
new CA2AppStack(app, "CA2Stack", {
  env: { region: "eu-west-1" },
});
