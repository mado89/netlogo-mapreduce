#!/bin/bash

VBoxManage startvm Master --type headless
VBoxManage startvm Node0 --type headless
VBoxManage startvm Node1 --type headless

