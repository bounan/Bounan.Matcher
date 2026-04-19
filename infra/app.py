#!/usr/bin/env python
from aws_cdk import App

from infra.matcher_cdk_stack import MatcherCdkStack


app = App()
MatcherCdkStack(app, "Bounan-Matcher")
app.synth()
