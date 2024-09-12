# 1. 从gotrackit导入相关模块Net, MapMatch
import pandas as pd
import geopandas as gpd
from gotrackit.map.Net import Net
from gotrackit.MapMatch import MapMatch


if __name__ == '__main__':

    # 1.读取GPS数据
    # 这是一个有10辆车的GPS数据的文件, 已经做过了数据清洗以及行程切分
    # 用于地图匹配的GPS数据需要用户自己进行清洗以及行程切分
    gps_df = pd.read_csv(r'../data/output/processed_公交轨迹数据.csv')
    print(gps_df)
    # gps_df = gps_df[gps_df['agent_id'] == 'xa_car_4']

    # 2.构建一个net, 要求路网线层和路网点层必须是WGS-84, EPSG:4326 地理坐标系
    # 请留意shp文件的编码，可以显示指定encoding，确保字段没有乱码
    link = gpd.read_file(r'../data/output/modified_roads0812.shp')
    node = gpd.read_file(r'../data/output/modified_nodes0812.shp')
    my_net = Net(link_gdf=link,node_gdf=node)
    my_net.init_net()  # net初始化

    # 3. 匹配
    mpm = MapMatch(net=my_net, gps_buffer=100, flag_name='xa_sample',
           use_sub_net=True, use_heading_inf=True, omitted_l=6.0,
           del_dwell=True, dwell_l_length=50.0, dwell_n=0,
           export_html=True, export_geo_res=True, use_gps_source=True,
           export_all_agents=False,
           out_fldr=r'../data/output', dense_gps=False,
           gps_radius=15.0)

    # 第一个返回结果是匹配结果表
    # 第二个是发生警告的相关信息
    # 第三个是匹配错误的agent的id编号列表
    match_res, may_error_info, error_info = mpm.execute(gps_df=gps_df)
    print(match_res)
    match_res.to_csv(r'../data/output/match_res.csv', encoding='utf_8_sig', index=False)