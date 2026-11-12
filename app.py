#!/usr/bin/env python3
import os
from dotenv import load_dotenv
load_dotenv()

import aws_cdk as cdk

from akic_iac.akic_ia_c_stack import AkicIaCStack


app = cdk.App()

cdk.Tags.of(app).add("project", "AKIC-Project")
cdk.Tags.of(app).add("name", "app-for-demo")

AkicIaCStack(app, "AkicIaCStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    # Account and Region are read from environment variables (AWS_ACCOUNT_ID / AWS_REGION)
    # so they are never hardcoded or pushed to git. Falls back to the current AWS CLI profile.
    env=cdk.Environment(
        account=os.getenv('AWS_ACCOUNT_ID', os.getenv('CDK_DEFAULT_ACCOUNT')),
        region=os.getenv('AWS_REGION', os.getenv('CDK_DEFAULT_REGION')),
    ),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

app.synth()
