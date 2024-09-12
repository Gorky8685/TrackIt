"""
9688872074 这node_id在
"""

import geopandas as gpd
import pandas as pd
import warnings

# 忽略RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning)


# 读取node和link文件
node = gpd.read_file(r'../data/output/nodes0812.shp')
link = gpd.read_file(r'../data/output/roads0812.shp')

print(node.crs,link.crs)
# 目标坐标系 EPSG:4326
crs_target = "EPSG:4326"
if node.crs != crs_target:
    node = node.to_crs(crs_target)

if link.crs != crs_target:
    link = link.to_crs(crs_target)

# 删除isvalid为-1的行
link = link[link['isvalid'] != '-1']

# 提取node中的id和osmid，建立映射关系
node_mapping = node[['osmid', 'id_0']].copy()  # 只提取所需的列
node_mapping['osmid'] = node_mapping['osmid'].round().astype('int64')  # 对osmid列进行四舍五入
node_mapping['id_0'] = node_mapping['id_0'].round().astype('int')
osmid_to_id_df = pd.DataFrame(list(zip(node_mapping['osmid'], node_mapping['id_0'])), columns=['osmid', 'id_0'])

# 保存 osmid_to_id 映射为 CSV
output_csv_path = r'../data/output/osmid_to_id_mapping.csv'
osmid_to_id_df.to_csv(output_csv_path, index=False)

# 对node操作：保留id列并生成新的node_id列
node['node_id'] = node['id_0'].astype('int')
# 保存处理后的node数据
node_output_path = r'../data/output/modified_nodes0812.shp'
node.to_file(node_output_path)
print(f"处理后的node文件已保存至: {node_output_path}")

# 确保link中的from和to列类型与osmid_to_id_df中的osmid列类型一致
link['from'] = link['from'].astype('int64')
link['to'] = link['to'].astype('int64')

# 1. 复制id列为link_id，转换为整数
link['link_id'] = link['id_0'].astype('int')

# 2. 使用 merge 方法，将 link 的 from 列与 osmid_to_id_df 的 osmid 列进行连接，生成 from_node
link = link.merge(osmid_to_id_df, left_on='from', right_on='osmid', how='left', suffixes=('', '_from'))
link['from_node'] = link['id_0_from']

# 3. 使用 merge 方法，将 link 的 to 列与 osmid_to_id_df 的 osmid 列进行连接，生成 to_node
link = link.merge(osmid_to_id_df, left_on='to', right_on='osmid', how='left', suffixes=('', '_to'))
link['to_node'] = link['id_0_to']

# 4. 输出没有匹配到的 from 和 to 值
unmatched_from = link[link['from_node'].isna()]['from']
if not unmatched_from.empty:
    print(f"未找到匹配的 from 值: {unmatched_from.tolist()}")

unmatched_to = link[link['to_node'].isna()]['to']
if not unmatched_to.empty:
    print(f"未找到匹配的 to 值: {unmatched_to.tolist()}")

# 5. 新建dir字段，值均为1，类型为长整型（int64）
link['dir'] = 1

# 6. 检查link_id是否有重复值，并输出重复的link_id
duplicated_link_ids = link[link['link_id'].duplicated()]['link_id']
if not duplicated_link_ids.empty:
    print(f"Warning: 存在重复的link_id: {duplicated_link_ids.tolist()}")
else:
    print("没有重复的link_id。")

# 7. 整理出现过的from_node和to_node
used_nodes = pd.concat([link['from_node'], link['to_node']]).unique()

# 8. 删除node表中未出现在used_nodes中的节点
node_cleaned = node[node['node_id'].isin(used_nodes)]

# 保存清理后的node数据
cleaned_node_output_path = r'../data/output/cleaned_nodes0812.shp'
node_cleaned.to_file(cleaned_node_output_path)
print(f"清理后的node文件已保存至: {cleaned_node_output_path}")

# 9. 保存处理后的link数据
link_output_path = r'../data/output/modified_roads0812.shp'
link.to_file(link_output_path)
print(f"处理后的link文件已保存至: {link_output_path}")
