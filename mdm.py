import pandas as pd
import os
import pyarabic.araby as araby
import tashaphyne.normalize as ts
import numpy as np
import json
import jaydebeapi
import hashlib
import traceback
import logging
from datetime import datetime
import time
import unicodedata

class cleaner:
    def __init__(self):
        return

    def update_df(self, df, df_m):
        edits_df = df_m.loc[df_m['action_id'] == 1]
        ExcludeWithoutDepend_df = df_m.loc[df_m['action_id'] == 2]

        for ind in edits_df.index:
            df.loc[(df['system_id'] == edits_df['system_id'][ind]) & (df['type_id'] == edits_df['type_id'][ind])
                   & (df['lookup_value'] == edits_df['lookup_value'][ind]), ['code', 'name_en', 'name_ar']] = [
                edits_df['code_x'][ind], edits_df['name_en_x'][ind], edits_df['name_ar_x'][ind]]

        for ind in ExcludeWithoutDepend_df.index:
            index = df[(df['system_id'] == ExcludeWithoutDepend_df['system_id'][ind]) & (
                        df['type_id'] == ExcludeWithoutDepend_df['type_id'][ind])
                       & (df['lookup_value'] == ExcludeWithoutDepend_df['lookup_value'][ind])].index
            df.drop(index, inplace=True, errors='ignore')

        return df

    def perpare_data(self, df, df_m):
        df = self.update_df(df, df_m)
        id = []
        names_ar_c = []
        names_en_c = []
        codes_c = []
        names_ar_v = []
        names_en_v = []
        codes_v = []
        codes = {}
        for ind in df.index:
            ret = self.clean_names(df['name_ar'][ind], df['name_en'][ind])
            names_ar_c.append(ret[0])
            names_en_c.append(ret[1])
            names_ar_v.append(ret[2])
            names_en_v.append(ret[3])



            if df['system_id'][ind] not in codes:
                codes[df['system_id'][ind]] = {}
            if df['code'][ind] is not None:
                codes[df['system_id'][ind]][df['code'][ind].strip().upper()] = 1

        for ind in df.index:
            if df['id'][ind] is not None:
                df.at[ind, 'id'] = str(df['id'][ind]).strip()

            system_id = df['system_id'][ind]
            code = df['code'][ind]
            if code is not None:
                code = code.strip().upper()
                if code.find('-') != -1:
                    code_wzout_org = code.split('-')[1].strip()
                    if code_wzout_org not in codes[system_id]:
                        code = code_wzout_org
                            
                if len(code) == 0:
                    code = None
            codes_v.append(code)
            codes_c.append(code)

        df['name_ar_c'] = names_ar_c
        df['name_en_c'] = names_en_c
        df['code_c'] = codes_c
        df['name_ar_v'] = names_ar_v
        df['name_en_v'] = names_en_v
        df['code_v'] = codes_v
        return df

    def is_contian_ar(self, str):
        if len(str) == 0:
            return False
        n=0
        for c in str:
            if araby.is_arabicrange(c):
                n+=1
        return n/(len(str)*1.0)>.5

    def is_contian_en(self, str):
        if len(str)==0:
            return False
        n = 0
        for c in str:
            if ord(c.lower())>=ord('a') and ord(c.lower())<=ord('z'):
                n+=1
        return n/(len(str)*1.0)>.5

    def remove_non_ar(self, str):
        ret = ""
        for c in str:
            if araby.is_arabicrange(c) or c == ' ':
                ret += c
        return ret

    def remove_non_en(self, str):
        ret = ""
        for c in str:
            if (ord(c.lower())>=ord('a') and ord(c.lower())<=ord('z')) or c == ' ':
                ret += c
        return ret

    def trim(self, str):
        return ' '.join(str.split())

    def order_lang(self, a, e):
        aa = ""
        ee = ""
        if self.is_contian_ar(a):
            aa = a
        elif self.is_contian_ar(e):
            aa = e

        if self.is_contian_en(e):
            ee = e
        elif self.is_contian_en(a):
            ee = a
        return aa, ee

    def clean_names(self, name_ar, name_en):
        if name_en is not None:
            name_en=name_en.strip()
        if name_ar is not None:
            name_ar=name_ar.strip()

        if name_en is None or name_en.lower() in ['test']:
            name_en = ""
        if name_ar is None:
            name_ar = ""

        name_ar, name_en = self.order_lang(name_ar, name_en)
        name_ar, name_en = self.remove_non_ar(name_ar), self.remove_non_en(name_en)
        name_ar_v, name_en_v = name_ar.strip(), name_en.capitalize().strip()
        name_ar = araby.strip_diacritics(name_ar)
        name_ar = ts.strip_tashkeel(name_ar)
        name_ar = ts.strip_tatweel(name_ar)
        name_ar = ts.normalize_hamza(name_ar)
        name_ar = ts.normalize_lamalef(name_ar)
        name_ar = ts.normalize_spellerrors(name_ar)
        name_en = name_en.lower()
        name_ar, name_en = name_ar.replace(' ', ''), name_en.replace(' ', '')
        if len(name_ar) == 0:
            name_ar = None
            name_ar_v = None
        if len(name_en) == 0:
            name_en = None
            name_en_v = None

        return name_ar, name_en, name_ar_v, name_en_v

class match_graph:
    def __init__(self, df, df_m,df_at,priority,rdm_io_obj,match_ids):
        self.systems={}
        self.rdm_io_obj=rdm_io_obj
        self.graph_candidates={}
        self.Group_id=1
        self.CantMatch = {}
        self.visited = {}
        self.component_priority={}
        self.match_ids=match_ids
        for c in priority:
            self.component_priority[c['component_type']]=c
        tree = {}
        for ind in df.index:
            if df['type_id'][ind] not in self.systems:
                self.systems[df['type_id'][ind]]=[]

            if df['system_id'][ind] not in self.systems[df['type_id'][ind]]:
                self.systems[df['type_id'][ind]].append(df['system_id'][ind])

            if df['type_id'][ind] not in tree:
                tree[df['type_id'][ind]] = []

            if df['parent_type_id'][ind] is not None:
                if df['parent_type_id'][ind] not in tree[df['type_id'][ind]]:
                    tree[df['type_id'][ind]].append(df['parent_type_id'][ind])

        tree_level = {}
        for tn in tree:
            tree_level[tn] = self.get_tree_level(tree, tn)

        self.graph = {}
        for ind in df.index:
            if df['code_c'][ind] is None and df['name_en_c'][ind] is None and df['name_ar_c'][ind] is None:
                continue
            key = "{}${}${}".format(df['type_id'][ind], df['system_id'][ind], df['lookup_value'][ind])
            parent_node_key = None
            if df['parent_lookup_value'][ind] is not None:
                parent_node_key = "{}${}${}".format(df['parent_type_id'][ind], df['system_id'][ind],
                                                    df['parent_lookup_value'][ind])
            self.graph[key] = {'tree_level': tree_level[df['type_id'][ind]],
                               'node': {
                                   'parent_key': parent_node_key,
                                   'type_id': df['type_id'][ind],
                                   'type_name': df['type_name'][ind],
                                   'system_id': df['system_id'][ind],
                                   'lookup_value': df['lookup_value'][ind],
                                   'id': df['id'][ind],
                                   'code': df['code'][ind],
                                   'name_ar': df['name_ar'][ind],
                                   'name_en': df['name_en'][ind],
                                   'code_c': df['code_c'][ind],
                                   'name_ar_c': df['name_ar_c'][ind],
                                   'name_en_c': df['name_en_c'][ind],
                                   'code_v': df['code_v'][ind],
                                   'name_ar_v': df['name_ar_v'][ind],
                                   'name_en_v': df['name_en_v'][ind]
                               },
                               'edges': {},
                               'attributes': {},
                               'group_id': [-1, 0]
                               }


        for ind in df_at.index:
            key = "{}${}${}".format(df_at['component_type_id'][ind], df_at['system_id'][ind], df_at['component_lookup_value'][ind])
            if key in self.graph:
                self.graph[key]['attributes'][df_at['attribute_name'][ind]]={
                    'system_id': df_at['system_id'][ind],
                    'attribute_category':df_at['attribute_category'][ind],
                    'attribute_type_id': df_at['attribute_type_id'][ind],
                    'attribute_lookup_value': df_at['attribute_lookup_value'][ind],
                    'attribute_value': df_at['attribute_value'][ind]
                }


        self.graph = dict(sorted(self.graph.items(), key=lambda item: item[1]['tree_level']))
        self.update_graph(df_m)

    def update_graph(self, df_m):
        CustomGroup_df = df_m.loc[df_m['action_id'] == 6]
        for ind in CustomGroup_df.index:
            key = "{}${}${}".format(CustomGroup_df['type_id'][ind], CustomGroup_df['system_id'][ind],
                                    CustomGroup_df['lookup_value'][ind])
            if key in self.graph:
                self.graph[key]['group_id'][0] = -CustomGroup_df['group_id_x'][ind]
                self.graph[key]['group_id'][1] = 1

        MatchWith_df = df_m.loc[df_m['action_id'] == 4]
        for ind in MatchWith_df.index:
            key1 = "{}${}${}".format(MatchWith_df['type_id'][ind], MatchWith_df['system_id'][ind],
                                     MatchWith_df['lookup_value'][ind])
            key2 = "{}${}${}".format(MatchWith_df['type_id'][ind], MatchWith_df['system_id_x'][ind],
                                     MatchWith_df['lookup_value_x'][ind])
            if key1 in self.graph and key2 in self.graph:
                self.graph[key1]['edges'][key2]=5
                self.graph[key2]['edges'][key1]=5

        NotMatchWith_df = df_m.loc[df_m['action_id'] == 5]
        for ind in NotMatchWith_df.index:
            key1 = "{}${}${}".format(NotMatchWith_df['type_id'][ind], NotMatchWith_df['system_id'][ind],
                                     NotMatchWith_df['lookup_value'][ind])

            key2 = "{}${}${}".format(NotMatchWith_df['type_id'][ind], NotMatchWith_df['system_id_x'][ind],
                                     NotMatchWith_df['lookup_value_x'][ind])
            if key1 in self.graph and key2 in self.graph:
                self.CantMatch[key1 + '*' + key2] = 1
                self.CantMatch[key2 + '*' + key1] = 1

        ExcludeWithDepend_df = df_m.loc[df_m['action_id'] == 3]
        ExcludeWithDependsKeys = {}
        for ind in ExcludeWithDepend_df.index:
            key = "{}${}${}".format(ExcludeWithDepend_df['type_id'][ind], ExcludeWithDepend_df['system_id'][ind],
                                    ExcludeWithDepend_df['lookup_value'][ind])
            if key in self.graph:
                ExcludeWithDependsKeys[key] = 1

        for key in self.graph:
            if self.graph[key]['node']['parent_key'] in ExcludeWithDependsKeys:
                ExcludeWithDependsKeys[key] = 1

        for key in ExcludeWithDependsKeys:
            del self.graph[key]

    def get_tree_level(self, tree, tn):
        if len(tree[tn]) == 0:
            return 0
        mx = -1
        for p in tree[tn]:
            mx = max(1 + self.get_tree_level(tree, p), mx)
        return mx

    def can_match(self, node1, node2):
        parent_group_1 = None
        parent_group_2 = None

        if node1 + '*' + node2 in self.CantMatch:
            return False

        if self.graph[node1]['node']['parent_key'] in self.graph:
            parent_group_1 = self.graph[self.graph[node1]['node']['parent_key']]['group_id'][0]
        if self.graph[node2]['node']['parent_key'] in self.graph:
            parent_group_2 = self.graph[self.graph[node2]['node']['parent_key']]['group_id'][0]
        return parent_group_1 == parent_group_2 \
               and self.graph[node1]['node']['type_id'] == self.graph[node2]['node']['type_id'] \
               and (self.graph[node1]['group_id'][1] == 0 and self.graph[node2]['group_id'][1] == 0)

    def match(self, node1, node2):
        node1 = self.graph[node1]['node']
        node2 = self.graph[node2]['node']
        name_ar_match = node1['name_ar_c'] == node2['name_ar_c'] and node1['name_ar_c'] is not None
        name_en_match = node1['name_en_c'] == node2['name_en_c'] and node1['name_en_c'] is not None
        code_match = node1['code_c'] == node2['code_c'] and node1['code_c'] is not None
        if code_match and name_en_match and name_ar_match:
            return 1  # code&name_ar&name_en
        if code_match and name_ar_match:
            return 2 # code&name_ar
        if code_match and name_en_match:
            return 3 # code&name_en
        if code_match:
            return 4  # code
        if name_ar_match:
            return 7 # name_ar
        if name_en_match:
            return 8  # name_en
        return -1

    def connected_nodes(self, node, edge_type_ids):
        if self.graph[node]['group_id'][1] == 0:
            self.graph[node]['group_id'][0] = self.Group_id
        self.visited[node] = 1
        for n in self.graph[node]['edges']:
            if self.graph[node]['edges'][n] not in edge_type_ids:
                continue
            if n in self.visited:
                continue
            self.connected_nodes(n, edge_type_ids)

    def match_nodes(self, nodes):
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                node1 = nodes[i]
                node2 = nodes[j]
                if not self.can_match(node1, node2):
                    continue
                match_code = self.match(node1, node2)
                if match_code == -1:
                    continue
                self.graph[node1]['edges'][node2]=match_code
                self.graph[node2]['edges'][node1]=match_code

        for n in nodes:
            if n not in self.visited:
                self.connected_nodes(n, self.match_ids)
                self.Group_id += 1

    def get_node_matches(self):
        group_nodes={}
        for node in self.graph:
            group_hash=self.graph[node]['group_hash']
            if group_hash not in group_nodes:
                group_nodes[group_hash]=[]
            group_nodes[group_hash].append(node)

        ret = []
        match_names = {
            1: "code&name_ar&name_en",
            2: "code&name_ar",
            3: "code&name_en",
            4: "code",
            5: "manual match",
            6: "indirect match",
            7: "name_ar",
            8: "name_en",
        }
        for group_hash in group_nodes:
            nodes=group_nodes[group_hash]
            for node1 in nodes:
                type_id = self.graph[node1]['node']['type_id']
                type_name = self.graph[node1]['node']['type_name']
                system_id_a = self.graph[node1]['node']['system_id']
                lookup_value_a = self.graph[node1]['node']['lookup_value']
                ss=[]
                for node2 in nodes:
                    if node1==node2:
                        continue
                    system_id_b = self.graph[node2]['node']['system_id']
                    lookup_value_b = self.graph[node2]['node']['lookup_value']

                    if system_id_b not in ss:
                        ss.append(system_id_b)

                    if node1 in self.graph[node2]['edges']:
                        match_id=self.graph[node2]['edges'][node1]
                    else:
                        match_id=6

                    match_name=match_names[match_id]
                    ret.append((type_id,type_name,system_id_a,lookup_value_a,system_id_b,lookup_value_b,match_id,match_name))

                for s in self.systems[type_id]:
                    if s not in ss:
                        ret.append((type_id,type_name, system_id_a, lookup_value_a, s, None, -1, 'no match'))

        return ret


    def build_matching_graph(self):
        prev_type_id = None
        current_nodes = []
        for idx, n in enumerate(self.graph):
            node = self.graph[n]['node']
            if (node['type_id'] != prev_type_id and prev_type_id is not None):
                self.match_nodes(current_nodes)
                current_nodes = []
                self.visited = {}
            prev_type_id = node['type_id']
            current_nodes.append(n)
        self.match_nodes(current_nodes)
        self.add_groups_hash()
        self.get_candidates()


    def add_groups_hash(self):
        group_graph = {}
        for node in self.graph:
            group_id=self.graph[node]['group_id'][0]
            if group_id not in group_graph:
                group_graph[group_id]=[]
            group_graph[group_id].append(node)

        for group_id in group_graph:
            nodes=group_graph[group_id]
            nodes.sort()
            str=""
            for node in nodes:
                str+=node+'**'
            hash=hashlib.md5(str.encode()).hexdigest()
            for node in nodes:
                self.graph[node]['group_hash']=hash

    def get_candidates(self):
        group_graph={}
        for node in self.graph:
            type_id=self.graph[node]['node']['type_id']
            system_id=self.graph[node]['node']['system_id']
            group_id=self.graph[node]['group_id'][0]
            if type_id not in group_graph:
                group_graph[type_id]={}
            if group_id not in group_graph[type_id]:
                group_graph[type_id][group_id]={}
            if system_id not in group_graph[type_id][group_id]:
                group_graph[type_id][group_id][system_id]=[]

            group_graph[type_id][group_id][system_id].append(node)

        for type in group_graph:
            for group in group_graph[type]:
                group_nodes_systems=group_graph[type][group]
                component_systems_order=self.component_priority[type]['component_systems_order']
                component_candidates=[]
                for sys_id in component_systems_order:
                    if sys_id in group_nodes_systems:
                        component_candidates=group_nodes_systems[sys_id]
                        break

                if len(component_candidates)==0:
                    for sys_id in group_nodes_systems:
                        for node in group_nodes_systems[sys_id]:
                            component_candidates.append(node)

                if (len(component_candidates)>1):
                    main_attribute_score={
                        'code_v':3,'name_ar_v':10,'name_en_v':2
                    }
                    mx=-1
                    mx_score_node=None
                    for can in component_candidates:
                        component_score=0
                        for attribute in main_attribute_score:
                            if self.graph[can]['node'][attribute] is not None:
                                component_score+=main_attribute_score[attribute]
                        if mx<component_score:
                            mx_score_node=can
                            mx=component_score
                    component_candidates=[mx_score_node]

                group_candidate=component_candidates[0]
                candidate_key=self.graph[group_candidate]['group_hash']
                self.graph_candidates[candidate_key]={
                    'system_id':0,
                    'type_id': self.graph[group_candidate]['node']['type_id'],
                    'type_name': self.graph[group_candidate]['node']['type_name'],
                    'code_v': self.graph[group_candidate]['node']['code_v'],
                    'name_ar_v': self.graph[group_candidate]['node']['name_ar_v'],
                    'name_en_v': self.graph[group_candidate]['node']['name_en_v'],
                    'attributes': {}
                }

                for at in self.graph[group_candidate]['attributes']:
                    self.graph_candidates[candidate_key]['attributes'][at]=self.graph[group_candidate]['attributes'][at]

                for at in self.graph_candidates[candidate_key]['attributes']:
                    attribute_systems_order=self.component_priority[type]['component_attributes'][at]
                    if attribute_systems_order[0]!=-1:
                        component_candidate_attributes=[]
                        for sys_id in attribute_systems_order:
                            if sys_id in group_nodes_systems:
                                for node in group_nodes_systems[sys_id]:
                                    component_candidate_attributes.append(self.graph[node]['attributes'][at])

                        if len(component_candidate_attributes)==0:
                            for sys_id in group_nodes_systems:
                                for node in group_nodes_systems[sys_id]:
                                    component_candidate_attributes.append(self.graph[node]['attributes'][at])
                                    

                        if len(component_candidate_attributes)>1:
                            for c_at in component_candidate_attributes:
                            
                                if c_at['attribute_value'] is not None or c_at['attribute_lookup_value'] is not None:
                                    self.graph_candidates[candidate_key]['attributes'][at]=c_at
                                    break
                        else:
                            self.graph_candidates[candidate_key]['attributes'][at] = component_candidate_attributes[0]

                    if self.graph_candidates[candidate_key]['attributes'][at]['attribute_category']==2:
                        self.graph_candidates[candidate_key]['attributes'][at]['attribute_candidate_value']=self.graph_candidates[candidate_key]['attributes'][at]['attribute_value']
                    else:
                        att_v=self.graph_candidates[candidate_key]['attributes'][at]
                        self.graph_candidates[candidate_key]['attributes'][at]['attribute_candidate_value']=None
                        if att_v['attribute_lookup_value'] is not None:
                            key = "{}${}${}".format(att_v['attribute_type_id'], att_v['system_id'],
                                                    att_v['attribute_lookup_value'])
                            hsh=self.graph[key]['group_hash']
                            self.graph_candidates[candidate_key]['attributes'][at]['attribute_candidate_value']=hsh

    def sort_nodes(self, n):
        return n[1], n[4], n[0]

    def write(self):
        rdm_component_groups_h=['group_lookup_value','system_id','type_id','type_name','lookup_value','sys_id','code','name_ar','name_en']
        rdm_component_groups_d=[]
        for node in self.graph:
            tup=(self.graph[node]['group_hash'],
                 self.graph[node]['node']['system_id'],
                 self.graph[node]['node']['type_id'],
                 self.graph[node]['node']['type_name'],
                 self.graph[node]['node']['lookup_value'],
                 self.graph[node]['node']['id'],
                 self.graph[node]['node']['code_v'],
                 self.graph[node]['node']['name_ar_v'],
                 self.graph[node]['node']['name_en_v'])
            rdm_component_groups_d.append(tup)


        if self.rdm_io_obj.write_type==3:
            if self.rdm_io_obj.excel_write('2',rdm_component_groups_h,rdm_component_groups_d)==0:
                return 0
        else:
            if self.rdm_io_obj.db_delete('2')==0:
                return 0
            if self.rdm_io_obj.db_insert('2',rdm_component_groups_h,rdm_component_groups_d)==0:
                return 0

        rdm_group_candidate_comp_h = ['group_lookup_value', 'type_id','type_name', 'code', 'name_ar', 'name_en']
        rdm_group_candidate_attr_h = ['group_lookup_value', 'type_id','type_name', 'attribute_name', 'attribute_value']
        rdm_group_candidate_comp_d = []
        rdm_group_candidate_attr_d = []
        for node in self.graph_candidates:
            tup = (node,
                   self.graph_candidates[node]['type_id'],
                   self.graph_candidates[node]['type_name'],
                   self.graph_candidates[node]['code_v'],
                   self.graph_candidates[node]['name_ar_v'],
                   self.graph_candidates[node]['name_en_v'])
            rdm_group_candidate_comp_d.append(tup)
            for att in self.graph_candidates[node]['attributes']:
                tup = (node,
                       self.graph_candidates[node]['type_id'],
                       self.graph_candidates[node]['type_name'],
                       att,
                       self.graph_candidates[node]['attributes'][att]['attribute_candidate_value'])
                rdm_group_candidate_attr_d.append(tup)


        if self.rdm_io_obj.write_type==3:
            if self.rdm_io_obj.excel_write('3',rdm_group_candidate_comp_h,rdm_group_candidate_comp_d)==0:
                return 0
        else:
            if self.rdm_io_obj.db_delete('3')==0:
                return 0
            if self.rdm_io_obj.db_insert('3', rdm_group_candidate_comp_h, rdm_group_candidate_comp_d)==0:
                return 0

        if self.rdm_io_obj.write_type==3:
            if self.rdm_io_obj.excel_write('4',rdm_group_candidate_attr_h,rdm_group_candidate_attr_d)==0:
                return 0
        else:
            if self.rdm_io_obj.db_delete('4')==0:
                return 0
            if self.rdm_io_obj.db_insert('4', rdm_group_candidate_attr_h, rdm_group_candidate_attr_d)==0:
                return 0

        rdm_component_matches_h = ['type_id','type_name', 'system_id_a', 'lookup_value_a', 'system_id_b', 'lookup_value_b', 'match_id', 'match_name']
        rdm_component_matches_d = self.get_node_matches()

        if self.rdm_io_obj.write_type==3:
            if self.rdm_io_obj.excel_write('1',rdm_component_matches_h,rdm_component_matches_d)==0:
                return 0
        else:
            if self.rdm_io_obj.db_delete('1')==0:
                return 0
            if self.rdm_io_obj.db_insert('1', rdm_component_matches_h, rdm_component_matches_d)==0:
                return 0

class rdm_io:
    def __init__(self,config):
        self.config=config

    def read_config(self):
        global log
        if 'write_connection' not in self.config:
            log.log_msg('write_connection not provided in config_file', 145)
            return 0
        if 'type' not in self.config['write_connection']:
            log.log_msg('write_connection.type not provided in config_file', 146)
            return 0
        write_temp={
                    "type": 1,"jdbc_class": 1,"jar_path": 1,"server": 1,"user": 1,"password": 1,"batch_size":1,
                    "tables": {
                        "1": 1,"2": 1,"3": 1,"4": 1
                    }
                }
        if self.config['write_connection']['type']==3:
            write_temp={"type": 1,
                        "tables": {"1": 1,"2": 1,"3": 1,"4": 1
                        }
                       }

        config_temp={
                "read_connection": {
                    "type": 1,"jdbc_class": 1,"jar_path": 1,"server": 1,"user": 1,"password": 1,"component_query_path": 1,
                    "component_attributes_query_path": 1
                },
                "write_connection": write_temp,
                "manual_file": {
                    "path": 1,"sheet": 1
                },
                "priority_file": {
                    "path": 1
                }
            }

        for i in config_temp:
            if i not in self.config:
                log.log_msg('{} not provided in config_file'.format(i), 103)
                return 0
            if config_temp[i]!=1:
                for j in config_temp[i]:
                    if j not in self.config[i]:
                        log.log_msg('{}.{} not provided in config_file'.format(i,j), 104)
                        return 0
                    if config_temp[i][j]!=1:
                        for k in config_temp[i][j]:
                            if k not in self.config[i][j]:
                                log.log_msg('{}.{}.{} not provided in config_file'.format(i, j, k), 105)
                                return 0

        if not os.path.exists(self.config['read_connection']['component_query_path']):
            log.log_msg('component query file not exist in {}'.format(self.config['read_connection']['component_query_path']), 106)
            return 0

        if not os.path.exists(self.config['read_connection']['component_attributes_query_path']):
            log.log_msg('component attributes query file not exist in {}'.format(self.config['read_connection']['component_attributes_query_path']), 107)
            return 0

        if not os.path.exists(self.config['manual_file']['path']):
            log.log_msg('manual_file not exist in {}'.format(self.config['manual_file']['path']), 108)
            return 0

        if not os.path.exists(self.config['priority_file']['path']):
            log.log_msg('priority_file not exist in {}'.format(self.config['priority_file']['path']), 109)
            return 0

        self.component_query = open(self.config['read_connection']['component_query_path'], "r").read()
        self.component_attributes_query = open(self.config['read_connection']['component_attributes_query_path'], "r").read()
        self.read_type = self.config['read_connection']['type']
        self.read_jdbc_class = self.config['read_connection']['jdbc_class']
        self.read_jar_path = self.config['read_connection']['jar_path']
        self.read_server = self.config['read_connection']['server']
        self.read_user = self.config['read_connection']['user']
        self.read_password = self.config['read_connection']['password']

        self.tables = self.config['write_connection']['tables']
        self.write_type = self.config['write_connection']['type']
        if self.config['write_connection']['type']!=3:
            self.write_batch_size = self.config['write_connection']['batch_size']
            self.write_jdbc_class = self.config['write_connection']['jdbc_class']
            self.write_jar_path = self.config['write_connection']['jar_path']
            self.write_server = self.config['write_connection']['server']
            self.write_user = self.config['write_connection']['user']
            self.write_password = self.config['write_connection']['password']

        self.manual_file = self.config['manual_file']
        self.priority_file = self.config['priority_file']

        if "match_methods" not in self.config:
            self.match_ids=[1,2,3,4,5,7,8]
        else:
            self.match_ids=[]
            for k in self.config['match_methods']:
                if self.config['match_methods'][k][0]=='Y':
                    self.match_ids.append(int(k))

        return 1

    def init_rw_connection(self):
        global log
        if self.read_type==1:
            self.input_conn,error = self.mssql_connection(self.read_server,self.read_user,self.read_password,self.read_jdbc_class,self.read_jar_path)
            if error!='':
                log.log_msg('error in creating the input mssql jdbc connection - {}'.format(error),110)
                return 0

        elif self.read_type==2:
            self.input_conn,error = self.impala_connection(self.read_server, self.read_user, self.read_password,self.read_jdbc_class,self.read_jar_path)
            if error!='':
                log.log_msg('error in creating the input impala jdbc connection - {}'.format(error),111)
                return 0

        else:
            log.log_msg('read method {} not supported'.format(self.read_type), 112)
            return 0

        if self.write_type == 1:
            self.write_conn, error = self.mssql_connection(self.write_server, self.write_user, self.write_password,
                                                           self.write_jdbc_class, self.write_jar_path)
            if error != '':
                log.log_msg('error in creating the write mssql jdbc connection - {}'.format(error), 113)
                return 0

        elif self.write_type == 2:
            self.write_conn, error = self.impala_connection(self.write_server, self.write_user, self.write_password,
                                                            self.write_jdbc_class, self.write_jar_path)
            if error != '':
                log.log_msg('error in creating the write impala jdbc connection - {}'.format(error), 114)
                return 0
        elif self.write_type == 3:
            pass
        else:
            log.log_msg('write method {} not supported'.format(self.write_user), 115)
            return 0

        return 1

    def mssql_connection(self,server,user,password,jdbc_class,jar_path):
        url = "jdbc:sqlserver://{}".format(server)
        try:
            conn = jaydebeapi.connect(jdbc_class,
                                                  "{};encrypt=true;trustServerCertificate=true;".format(url),
                                                  [user, password],
                                                  jar_path)
            return conn,''
        except Exception as e:
            return None,repr(e)

    def impala_connection(self,server,user,password,jdbc_class,jar_path):
        url=server
        try:
            conn = jaydebeapi.connect(jdbc_class,
                                      "{};".format(url),
                                      {'UID': user, 'PWD': password},
                                      jar_path)                         
            return conn,''
        except Exception as e:
            return None,repr(e)
                  

    def db_select(self,select_query):
        try:
            curs = self.input_conn.cursor()
            curs.execute(select_query)
        except Exception as e:
            return None,repr(e)
        columns = [desc[0] for desc in curs.description]
        df = pd.DataFrame(curs.fetchall(), columns=columns)
        df = df.fillna(np.nan).replace([np.nan], [None])
        for ind in df.index:
            for c in df.columns:
                if type(df[c][ind]) is float:
                    df.at[ind, c] = int(df[c][ind])

        return df,''

    def db_delete(self,table_id):
        curs = self.write_conn.cursor()
        table_name = self.tables[table_id]
        delete_query = """delete from {}""".format(table_name)
        try:
        
            curs.execute(delete_query)
            pass
        except Exception as e:
            log.log_msg("an error occurred while deleting from {} table : {}".format(table_name,repr(e)),142)
            return 0
        return 1

    def db_insert(self,table_id,columns,data):
        curs=self.write_conn.cursor()
        table_name=self.tables[table_id]
        insert_query="""insert into {} ({})
                values ({})""".format(table_name,','.join(columns),",".join("?" for i in range(1,len(columns)+1)))
        try:
            batchsize=self.write_batch_size
            startpos=0
            while startpos<len(data):
                curs.executemany(insert_query, data[startpos:startpos+batchsize])
                self.write_conn.commit()
                startpos+=batchsize
                
        except Exception as e:
            log.log_msg("an error occurred while inserting into {} table : {}".format(table_name,repr(e)),143)
            return 0
        return 1

    def excel_write(self,table_id,columns,data):
        table_name = self.tables[table_id]
        try:
            df = pd.DataFrame(data, columns=columns)
            df.to_excel(table_name, index=False)
        except Exception as e:
            log.log_msg("an error occurred while inserting into {} : {}".format(table_name,repr(e)),147)
            return 0
        return 1

    def excel_read(self, file_path, sheet):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet)
        except Exception as e:
            return None,repr(e)
        df = df.fillna(np.nan).replace([np.nan], [None])
        for ind in df.index:
            for c in df.columns:
                if type(df[c][ind]) is float:
                    df.at[ind, c] = int(df[c][ind])

        return df,''

    def rdm_read_data(self):
        global log
        if self.read_config()==0:
            return 0,None,None,None,None,None

        if self.init_rw_connection()==0:
            return 0,None,None,None,None,None

        df,error = self.db_select(self.component_query)
        if error!='':
            log.log_msg('error while executing component query - {}'.format(error), 116)
            return 0,None,None,None,None,None

        comp_exp_columns=['system_id','type_id','parent_type_id','parent_lookup_value','lookup_value','id','code','name_ar','name_en']
        for c in comp_exp_columns:
            if c not in df.columns:
                log.log_msg('column ({}) not returned from component query'.format(c), 117)
                return 0, None, None, None, None,None

        duplicate_rows = df.duplicated(subset=['system_id', 'type_id','lookup_value']).any()
        if duplicate_rows:
            log.log_msg('system_id, type_id, lookup_value are not unique in the returned from component query'.format(c), 118)
            return 0, None, None, None, None,None

        df_at,error = self.db_select(self.component_attributes_query)
        if error!='':
            log.log_msg('error while executing component attributes query - {}'.format(error), 119)
            return 0,None,None,None,None,None

        comp_at_exp_columns = ['system_id', 'component_type_id', 'component_lookup_value', 'attribute_category', 'attribute_name', 'attribute_type_id',
                            'attribute_lookup_value', 'attribute_value']
        for c in comp_at_exp_columns:
            if c not in df_at.columns:
                log.log_msg('column ({}) not returned from component attributes query'.format(c), 120)
                return 0, None, None, None, None,None

        duplicate_rows = df_at.duplicated(subset=['system_id', 'component_type_id', 'component_lookup_value','attribute_name']).any()
        if duplicate_rows:
            log.log_msg(
                'system_id, component_type_id, component_lookup_value, attribute_name are not unique in the returned from component attributes query'.format(c), 121)
            return 0, None, None, None, None,None

        df_m,error = self.excel_read(self.manual_file['path'],self.manual_file['sheet'])
        if error!='':
            log.log_msg('error while reading manual file - {}'.format(error), 121)
            return 0,None,None,None,None,None

        manual_edits_columns=['system_id','type_id','lookup_value','action_id','action','system_id_x','group_id_x','lookup_value_x','code_x','name_en_x','name_ar_x']
        for c in manual_edits_columns:
            if c not in df_m.columns:
                log.log_msg('column ({}) not not found in manual file'.format(c), 122)
                return 0, None, None, None, None,None

        try:
            with open(self.priority_file['path']) as json_file:
                priority = json.load(json_file)
        except Exception as e:
            log.log_msg("error while converting priority file to json {}".format(repr(e)),123)
            return 0,None,None,None,None,None

        if 'components' not in priority:
            log.log_msg("components key not exist in priority json", 124)
            return 0, None, None, None, None,None

        if type(priority['components'])!=list:
            log.log_msg("components expected to be list in priority json", 125)
            return 0, None, None, None, None,None

        priority_items=['component_type','component_systems_order','component_attributes']
        for y in priority['components']:
            if type(y)!=dict:
                log.log_msg("items in priority component must be dict in priority json", 126)
                return 0, None, None, None, None,None
            for x in priority_items:
                if x not in y:
                    log.log_msg("{} not exist in one of priority component items dict in priority json".format(x), 127)
                    return 0, None, None, None, None,None

            if type(y['component_systems_order'])!=list:
                log.log_msg("component_systems_order must be list in priority json", 128)
                return 0, None, None, None, None,None

            if type(y['component_type'])!=int:
                log.log_msg("component_type must be int in priority json", 129)
                return 0, None, None, None, None,None


            if type(y['component_attributes'])!=dict:
                log.log_msg("component_attributes must be dict in priority json", 130)
                return 0, None, None, None, None,None

            for z in y['component_attributes']:
                if type(y['component_attributes'][z])!=list:
                    log.log_msg("component_attributes items {} must be list in priority json".format(z), 131)
                    return 0, None, None, None, None,None

        return self.validate_inputs(df, df_at, df_m, priority['components'],self.match_ids)

    def validate_inputs(self,df,df_at, df_m, priority,match_ids):
        components_l={}
        components_parent_l={}
        components_types_l={}

        for ind in df.index:
            components_types_l[df['type_id'][ind]]=[]
            key = "{}${}${}".format(df['type_id'][ind], df['system_id'][ind], df['lookup_value'][ind])
            components_l[key]=1
            if df['parent_type_id'][ind] is not None:
                key_p = "{}${}${}".format(df['parent_type_id'][ind], df['system_id'][ind], df['parent_lookup_value'][ind])
                components_parent_l[key_p]=key

        for k in components_parent_l:
            if k not in components_l:
                log.log_msg("{} used as component parent to {} but it is not exist in components".format(k,components_parent_l[k]), 132)
                return 0, None, None, None, None, None

        for ind in df_at.index:
            key1 = "{}${}${}".format(df_at['component_type_id'][ind], df_at['system_id'][ind],
                                     df_at['component_lookup_value'][ind])

            if key1 not in components_l:
                log.log_msg(
                    "component {} exist in components attributes but not exist in components query".format(key1), 133)
                return 0, None, None, None, None, None

            if df_at['attribute_category'][ind]==1 and df_at['attribute_lookup_value'][ind] is not None:
                key2 = "{}${}${}".format(df_at['attribute_type_id'][ind], df_at['system_id'][ind], df_at['attribute_lookup_value'][ind])
                if key2 not in components_l:
                    log.log_msg("component attribute lookup{} {} not exist in components query".format(key1,key2), 134)
                    return 0, None, None, None, None, None


            components_types_l[df_at['component_type_id'][ind]].append(df_at['attribute_name'][ind])

        for type_id in components_types_l:
            comp=None
            for c in priority:
                if c['component_type']==type_id:
                    comp=c
            if comp is None:
                log.log_msg("component type {} not exist in priority file".format(type_id), 135)
                return 0, None, None, None, None, None

            for a in components_types_l[type_id]:
                if a not in comp['component_attributes']:
                    log.log_msg("component attribute {} not exist in priority file for component {}".format(a,type_id), 136)
                    return 0, None, None, None, None, None

        r_ind=[]
        for ind in df_m.index:
            action_id=df_m['action_id'][ind]
            if action_id not in [1,2,3,4,5,6]:
               r_ind.append(ind)
               log.log_msg("manual action id {} not valid".format(action_id),137,'warn')
               continue

            key = "{}${}${}".format(df_m['type_id'][ind], df_m['system_id'][ind], df_m['lookup_value'][ind])
            if key not in components_l:
                r_ind.append(ind)
                log.log_msg("manual action component {} not exist in component query".format(key), 138, 'warn')
                continue

            if action_id==1 and (df_m['code_x'][ind] is None or df_m['name_ar_x'][ind] is None or df_m['name_en_x'][ind] is None):
                r_ind.append(ind)
                log.log_msg("for manual action 1, code_x, name_ar_x, name_en_x must be filled".format(key), 139, 'warn')
                continue

            key2="{}${}${}".format(df_m['type_id'][ind], df_m['system_id_x'][ind], df_m['lookup_value_x'][ind])
            if action_id in [4,5] and key2 not in components_l:
                r_ind.append(ind)
                log.log_msg("x attributes with key {} not found in the components for action {}".format(key2,action_id), 140, 'warn')
                continue

            if action_id==6 and type(df_m['group_id_x'][ind])!=int:
                r_ind.append(ind)
                log.log_msg(
                    "for manual action 6, group_id_x must be int", 141,'warn')
                continue

        df_m=df_m.drop(index=r_ind)

        return 1,df,df_at, df_m, priority,match_ids

def rdm_engine():
    global log
    try:
      if not os.path.exists('config.json'):
          log.log_msg("config file not exist in current workspace",101)
          return 0
      try:
          with open('config.json') as json_file:
              config = json.load(json_file)
      except Exception as e:
          log.log_msg("error while converting config to json {}".format(repr(e)),102)
          return 0
  
      rdm_io_obj = rdm_io(config)
      status,df,df_at, df_m,priority,match_ids = rdm_io_obj.rdm_read_data()

      #test input
      hashs=[]
      for ind in df.index:
          concat="{}${}${}${}${}${}${}${}${}".format(df['system_id'][ind],df['type_name'][ind],df['parent_type_id'][ind],df['parent_lookup_value'][ind],
                                                     df['lookup_value'][ind],df['id'][ind],df['code'][ind],df['name_ar'][ind],df['name_en'][ind])
          hash = hashlib.md5(concat.encode()).hexdigest()
          hashs.append(hash)
      df['hash']=hashs
      df.to_excel('input_hashed_{}.xlsx'.format(time.time()),sheet_name='sheet1')
      #end test input

      if status==0:
          return 0
  
      cl = cleaner()
      df = cl.perpare_data(df, df_m)
      mg = match_graph(df, df_m,df_at,priority,rdm_io_obj,match_ids)
      mg.build_matching_graph()
      if mg.write()==0:
            return 0
    except Exception as e:
        log.log_msg("an error occurred : {}".format(repr(e)),144)
        return 0

    return 1

class logger:
    def __init__(self):
        #logging.basicConfig(filename='log.log', encoding='utf-8', level=logging.DEBUG,filemode='w')
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('log.log','w','utf-8')
        root_logger.addHandler(handler)

    def log_msg(self,msg,code,lvl='error'):
        msg = "{} - {} - {}".format(datetime.now(),code,msg)
        if lvl=='error':
            logging.error(msg)
        elif lvl=='info':
            logging.info(msg)
        elif lvl=='warn':
            logging.warning(msg)



log = logger()
x=rdm_engine()
print(x)
