# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
# Testscript Name: dp_stcli_migrate.py
# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
# Steps:
# ------------------------------------------------------------------------------
# Perform below on source and destination testbed
# 1. Create cluster
# 2. Create datastore
# 3. Deploy workload vms
# 5. Setup DR network
#
# Perform below on source
# 6. Pair clusters
# 7. Pair datastores
#

# Tst12042338c	stcli_migrate with valid vmid which is configured with stdSwitch (source and target not having same network names )
# Tst12042339c	stcli_migrate with valid vmid which is configured with vDS (source and target not having same network names )
# Tst12042340c	stcli_migrate  with valid vmid --networkmapping(stdSwitch) ( invalid )
# Tst12042341c	stcli_migrate  with valid vmid --networkmapping(vDS) ( invalid )
# Tst12042344c	stcli_migrate with valid vmid  --network mapping when source is invalid
# Tst12042342c	stcli_migrate  with valid vmid --networkmapping(stdSwitch => vDS) ( invalid )
# Tst12042343c	stcli_migrate  with valid vmid --networkmapping(vDS => stdSwitch) ( invalid )
# Tst12042349c	stcli_migrate  with valid group VM vmid --networkmapping(stdSwitch) ( invalid )
# Tst12042352c	stcli_migrate  with valid group VM vmid --networkmapping(vDS => stdSwitch) ( invalid )
# Tst12042351c	stcli_migrate  with valid group VM vmid --networkmapping(stdSwitch => vDS) ( invalid )
# Tst12042350c	stcli_migrate  with valid group VM vmid --networkmapping(vDS) ( invalid )
# New testcases being added


import time
import os
import traceback
from setup import get_testbeds, get_test_section_name
from replication.native_replication_factory import NativeReplicationFactory
from replication.helper.nr_util import balance_vms
from test_util.helper import log_test_step
from log_wrapper import *
import pdb

test_description = ""
test_params = {'description': test_description,
               'num_instances': 1,
               'runstorfs': False}

repl_helper = None
sp = None
tsp = None
testbeds = None
logger = None
schedule = None
user_vms = None
src_DVS_name = 'src-DVS'
tgt_DVS_name = 'tgt-DVS'
src_DVS_name1 = 'DUMMY-DVS'
src_std_switch = 'Storage Controller Data Network'
tgt_std_switch = 'Storage Controller Management Network'
src_DVS_pg_name = '{}-pg1'.format(src_DVS_name)
tgt_DVS_pg_name = '{}-pg1'.format(tgt_DVS_name)
src_std_switch1 = 'vm-network-410'
src_DVS_pg_name1 = '{}-pg1'.format(src_DVS_name1)


def setup(args):
    global repl_helper, testbeds, tc_config_dst, logger, schedule, sp, tsp, \
        user_vms, rp, folder

    testloc = os.path.abspath(__file__)

    logger = args['logger']
    logger.info('Starting Setup')
    # Get testbeds required to run this test
    testbeds = get_testbeds(args)
    if len(testbeds) != 2:
        logger.error("Need exactly 2 testbeds to run this test")
        return -1
    logger.info('Getting Native factory')
    logger.info('>>> src testbed :{}'.format(testbeds[0]))
    logger.info('>>> dst testbed :{}'.format(testbeds[1]))
    repl_helper = NativeReplicationFactory(nr_type='stcli',
                                           source_tb=testbeds[0],
                                           destination_tb=testbeds[1],
                                           logger=logger)
    # ------------------------------------------------------------------------
    # Step 1 : Create cluster on source and destination
    # Step 2 : Create datastores
    # Step 3 : Deploy workload VMs
    # Step 4 : Start workload
    # Step 5 : Setup DR Network
    # Step 6 : Pair Clusters
    # Step 7 : Pair Datastores
    # -------------------------------------------------------------------------
    repl_helper.dr_setup(args, testloc, start_workload_on='None',
                         deploy_vms_on='source', power_on_vms=False)

    tc_config = repl_helper.src_config
    tc_config_dst = repl_helper.dst_config
    schedule = [
        {
            "enabled": True,
            "interval_in_minutes": tc_config['schedule_interval'],
            "start_time": 0,
            "quiesce_type": "NONE"
        }
    ]
    sp = repl_helper.source_sp_cluster.get_rest_interface()
    tsp = repl_helper.dest_sp_cluster.get_rest_interface()
    logger.info('Getting user vms')
    user_vms = sp.list_vms(option='unprotected')
    vm = user_vms[0]
    vm_name = vm.name

    total_vm_cnt = 11
    vms_required = total_vm_cnt - len(user_vms) \
        if len(user_vms) < total_vm_cnt else 1

    job = sp.vm_clone(
        vm.moid,
        vm_name,
        vm_name,
        vms_required,
        clone_name_start_number=1,
        clone_name_increment=1)
    job.wait_for_completion(verify_summary_step_state='SUCCEEDED')

    vm_names = [vm.name for vm in sp.list_vms()]
    sp_vms = [
        sp.dp_vm_protect(
            vm_er=sp.dp_vm_er(x),
            target_sp_cl=tsp,
            schedule=schedule) for x in vm_names]

    for vm in sp_vms:
        # Logic is to check if the VM is successfully replicated once
        # if yes then we do not wait for replication.
        if vm.is_successfully_replicated_once:
            logger.info('VM "{}" replication is already done'.format(vm.name))
        else:
            vm.wait_for_replication(status='SUCCESS')
            logger.info('VM "{}" replication is done'.format(vm.name))

    user_vms = sp.user_vms
    # create resourcepool and folder
    rp_name = tc_config_dst['resource_pool']
    folder_name = tc_config_dst['folder']
    tsp.vw_cl.enable_drs()
    try:
        rp = tsp.vw_cl.create_resource_pool(rp_name, \
                                            memory_expandable_reservation=True)
        log_test_step('Created Resource Pool: {}'.format(rp.name))
    except Exception as e:
        if ' already exists' in str(e):
            logger.info('Using exisiting resource pool:{}'.format(rp_name))
            rp = [rp for rp in tsp.vw_cl.pvm_cl.resourcePool.resourcePool \
                  if rp.name == rp_name][0]
            log_test_step('Found Resource Pool: {}'.format(rp.name))
        else:
            raise Exception('Failed to create RP:{}'.format(e))

    try:
        folder = tsp.vw_dc.createVmFolder(folder_name)
        log_test_step('Created folder:{}'.format(folder.name))
    except Exception as e:
        if 'DuplicateName' in str(e):
            logger.info('Using existing folder')
            folder = [folder for folder in \
                      tsp.vw_dc.pvm_dc.vmFolder.childEntity \
                      if folder.name == folder_name][0]
            log_test_step('Found folder:{}'.format(folder.name))
        else:
            raise Exception('Failed to create Folder:{}'.format(e))

    # setup network
    # create DVS on src/tgt
    #Using version here is an work-around as there is noncompatibility of the switch with ESXi version , it
    #Eventually throws warnings and that warning returns False for script validation.
    assert sp.vw_dc.create_distributed_virtual_switch(src_DVS_name, \
                            sp.vw_cl.hosts, version='6.5.0',new_port_group_name=src_DVS_pg_name), \
        'Failed to create DVS on source'
    assert tsp.vw_dc.create_distributed_virtual_switch(tgt_DVS_name, \
                            tsp.vw_cl.hosts, version='6.5.0',new_port_group_name=tgt_DVS_pg_name), \
        'Failed to create DVS on target'

    return 0


def migrate_and_verify(vm, tc_id, src_network=None, tgt_network=None, group=None,
                       folder_id=None, folder_name=None, power_on=None,
                       resourcepool_id=None, resourcepool_name=None):
    """
        Helper function to perform migrate and verify
    """
    log_test_step("Executing TC: {}".format(tc_id))
    logger.info('Using VM {}'.format(vm.name))
    supported_options = ['folder_id', 'folder_name', \
                         'resourcepool_id', 'resourcepool_name',
                         'power_on']
    migrate_options = {}
    for option in supported_options:
        if eval(option):
            migrate_options[option] = eval(option)

    if src_network and tgt_network:
        migrate_options['network_mapping'] = \
            '"{}":"{}"'.format(src_network, tgt_network)
        dvs = True if 'DVS' in src_network else False
        logger.info('Migrating src VM:{}, is_vds:{}, map:{}'. \
                    format(vm.name, str(dvs), migrate_options['network_mapping']))
        assert vm.change_network_portgroup_to(src_network, is_vds=dvs), \
            'Failed to change network settings of VM:{}'.format(vm.name)

    if group:
        if group in sp.dp_groups_by_name:
            pg = sp.dp_groups_by_name[group]
            logger.info('Selecting group:{}'.format(pg.name))
        else:
            pg = sp.dp_group_create(name=group, target_sp_cl=tsp,
                                    schedule=schedule)
            logger.info('Created group:{}'.format(pg.name))
        pg.modify(vms_to_move_in=sp.dp_vm_ers([vm.name]))
        for v in pg.vms:
            logger.info('VMs in the pg1 group before removing:{}'.format(v.name))
        repl_helper.moveout_vms_from_group(sp=sp, group_name=group, vms=[vm.name])
        for v in pg.vms:
            logger.info('VMs in the pg1 group after removing:{}'.format(v.name))
    logger.info('migrating VM')
    repl_helper.migrate_vms(sp=repl_helper.dest_sp_cluster, vms=[vm],
                            **migrate_options)
    logger.info('Migarated VM. Sleeping for 20 secs')
    time.sleep(20)
    tvm = [tvm for tvm in tsp.user_vms if tvm.name == vm.name]
    assert tvm, 'VM {} did not recover on target'.format(vm.name)
    if src_network and tgt_network:
        network = tvm[0].pvm_vm.network
        logger.info('Target recovered VM:{}, network:{}'. \
                    format(tvm[0].name, [n.name for n in network]))
        assert [pg.name for pg in network if pg.name == tgt_network], \
            'TC FAILED while verifying network, vm netowrks:{}'. \
                format([n.name for n in network])
    if folder_id or folder_name:
        _assert_vm_in_folder(vm.name)
    if resourcepool_id or resourcepool_name:
        _assert_vm_in_respool(vm.name)
    if power_on:
        assert tvm[0].is_powered_on, \
            'Failed to power on VM:{}'.format(tvm[0].name)

    log_test_step("TESTCASE PASSED: {}".format(tc_id))

    return 0

def run(args):
    total_test_cases = 20
    vm_cnt = 0
    tgt_std_switch1 = 'dummy' 
    src_std_switch1 = tgt_std_switch1
    tgt_DVS_pg_name1 = tgt_std_switch1


    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042340c	stcli_migrate \
                with valid vmid --networkmapping(stdSwitch) ( invalid )",
                src_network=src_std_switch, tgt_network=tgt_std_switch1)
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_std_switch1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042342c	stcli_migrate  \
                with valid vmid --networkmapping(stdSwitch => vDS) ( invalid )",
                src_network=src_std_switch, tgt_network=tgt_DVS_pg_name1)
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_DVS_pg_name1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042344c	stcli_migrate \
                with valid vmid  --network mapping when source is invalid",
                src_network=src_std_switch1, tgt_network=tgt_std_switch)
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(src_std_switch1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042349c	stcli_migrate \
                with valid group VM vmid --networkmapping(stdSwitch) ( invalid )",
                src_network=src_std_switch1, tgt_network=tgt_std_switch, group='pg1')
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(src_std_switch1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042351c	stcli_migrate \
                with valid group VM vmid --networkmapping(stdSwitch => vDS) ( invalid )",
                src_network=src_std_switch, tgt_network=tgt_DVS_pg_name1, group='pg1')
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_DVS_pg_name1))

    migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042338c	stcli_migrate with  \
                valid vmid which is configured with stdSwitch (source and target not \
                having same network names )",src_network=src_std_switch1)

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042341c	stcli_migrate \
                with valid vmid --networkmapping(vDS) ( invalid )",
                src_network=src_DVS_pg_name, tgt_network=tgt_DVS_pg_name1)
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_DVS_pg_name1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042343c	stcli_migrate \
                with valid vmid --networkmapping(vDS => stdSwitch) ( invalid )",
                src_network=src_DVS_pg_name, tgt_network=tgt_std_switch1)
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_std_switch1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042350c	stcli_migrate \
                with valid group VM vmid --networkmapping(vDS) ( invalid )",
                src_network=src_std_switch, tgt_network=tgt_DVS_pg_name1, group='pg1')
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_DVS_pg_name1))

    vm_cnt += 1

    try:
        migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042352c	stcli_migrate \
                with valid group VM vmid --networkmapping(vDS => stdSwitch) ( invalid )",
                src_network=src_DVS_pg_name, tgt_network=tgt_std_switch1, group='pg1')
    except Exception as e:
        log_h2("Exception is {}".format(e))
        _assert_error(e, 'Failed during recover validations: Target Network of name {0} does not exist'.format(tgt_std_switch1))

    vm_cnt += 1

    migrate_and_verify(vm=user_vms[vm_cnt], tc_id="Tst12042339c	stcli_migrate with valid \
                vmid which is configured with vDS (source and target not having \
                same network names )",src_network=src_DVS_pg_name)

    return 0


def ignore_error(func):
    def wrapped(*args, **kwargs):
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            log_warning('Exception:{}'.format(e))
        return ret
    return wrapped

@ignore_error
def unprotect_all_vms(sp, tsp):
    spdpvms = sp.dp_vms
    tspdpvms = tsp.dp_vms
    for i in xrange(len(spdpvms)):
        if spdpvms[i].unprotect(target_sp_cl=tsp):
            log_h2('Unprotected src VM:{}'.format(spdpvms[i].name))
        elif tspdpvms[i].unprotect(target_sp_cl=sp):
            log_h2('Unprotected tgt VM:{}'.format(tspdpvms[i].name))
        else:
            logger.error('Undefined')
    for grp in sp.dp_groups:
        logger.info('Deleting grp: {}'.format(grp.name))
        grp.delete()
    for grp in tsp.dp_groups:
        logger.info('Deleting grp: {}'.format(grp.name))
        grp.delete()

@ignore_error
def delete_dvs():
    log_h2('Delete DVS:{} on {}'.format(src_DVS_name, sp.name))
    assert sp.vw_dc.delete_dvs(src_DVS_name), 'Failed to delete DVS:{}'.format(src_DVS_name)
    log_h2('Delete DVS:{} on {}'.format(src_DVS_name, tsp.name))
    assert tsp.vw_dc.delete_dvs(tgt_DVS_name), 'Failed to delete DVS:{}'.format(tgt_DVS_name)

@ignore_error
def change_vm_network_settings_to_default(sp, is_vds=False):
    for vm in sp.list_vms()[0:4]:
        log_h2('Changing to VM Network for VM:{}'.format(vm.name))
        assert vm.change_network_portgroup_to('VM Network', is_vds=is_vds)

def _assert_vm_in_respool(vm_name):
    assert vm_name in [x.name for x in rp.vm], vm_name + \
        ' not found in resource_pool: ' + rp.name

def _assert_vm_in_folder(vm_name):
    assert vm_name in [x.name for x in folder.childEntity], vm_name + \
        ' not found in folder: ' + folder.name

def _assert_error(e, err):
    assert err in e.message, 'err msg not found in ' + e.message

def cleanup(args):
    """
    Cleanup

    Args:
        None
    """

    log_test_step(" Cleanup ")
    change_vm_network_settings_to_default(sp)
    change_vm_network_settings_to_default(tsp)
    unprotect_all_vms(sp, tsp)
    log_h2("Delete user  VM on source cluster...if any")
    sp.destroy_user_vms()
    log_h2("Delete user VM on  target cluster...if any")
    tsp.destroy_user_vms()
    delete_dvs()
    return 0

