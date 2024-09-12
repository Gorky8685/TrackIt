"""
路网数据要求！
路网点层数据和线层数据的坐标系必须为：EPSG:4326
node_id link_id 字段类型int
点层 node_id, geometry (POINT (108.84059274796762 34.20380728755708))
dir=1 代表路段拓扑正向
线层 link_id, from_node, to_node，dir, length, geometry (LINESTRING (108.84048418194278 34.208751404812496, 108.8410333043887 34.20538952458989))
可以再加个road_name
线层表的geometry字段中不允许出现MultiLineString类型，只允许LineString类型，不支持三维坐标。两个node之间的link由多个经纬度坐标组成。

GPS定位数据要求：
agent_id, lng, lat, time (2024-01-15 16:00:29  ”%Y-%m-%d %H:%M:%S”可以参照pandas中pd.to_datetime()函数的format参数)
time列用于确定点的先后顺序，不允许有重复值，未来的版本会使用time列计算速度用于提供ST-MATCHING。
如果你的数据没有时间列，请自己赋予一个时间列且确保没有重复值。##这里我们后面计算Schedule是不是可以用高德api计算时间。

gotrackit不允许路网出现环路以及(from_node，to_node)相同的link存在(如下图), 在构建Net时会自动识别这些link并且进行删除, 如果你想保留这些link请使用circle_process进行路网处理

其时间差超过group_gap_threshold，则在此处切分主行程。默认1800s(30分钟)
子行程 如果超过连续n个gps点的距离小于min_distance_threshold 且 持续时间超过dwell_accu_time，那么该处被识别为停留点，从该处切分子行程。如果你只想划分主行程，则指定min_distance_threshold为负数即可
n，默认5。dwell_accu_time，默认60秒
"""
import os
import geopandas as gpd
import gotrackit.netreverse.NetGen as ng
from gotrackit.map.Net import Net
from gotrackit.MapMatch import MapMatch
from gotrackit.generation.SampleTrip import TripGeneration



# 基于已有标准路网, 检查路网的联通性并进行修复
if __name__ == '__main__':
    link_gdf = gpd.read_file(r'./data/input/net/test/sz/FinalLink.shp')
    node_gdf = gpd.read_file(r'./data/input/net/test/sz/FinalNode.shp')

    # net_file_type指的是输出路网文件的类型
    nv = ng.NetReverse(net_file_type='shp', conn_buffer=0.8, net_out_fldr=r'./data/input/net/test/sz/')
    new_link_gdf, new_node_gdf = nv.modify_conn(link_gdf=link_gdf, node_gdf=node_gdf, book_mark_name='sz_conn_test', generate_mark=True)

    print(new_link_gdf)
    print(new_node_gdf)


"""
del_dwell_points(): 停留点删除
    dwell_l_length: 停留点识别距离阈值, 默认值5.0m
    dwell_n: 超过连续dwell_n个相邻GPS点的距离小于dwell_l_length，那么这一组点就会被识别为停留点，默认2

dense(): 轨迹点增密
    dense_interval: 当相邻GPS点的球面距离L超过dense_interval即进行增密, 进行 int(L / dense_interval) + 1 等分加密, 默认100.0

lower_frequency(): 轨迹点降频
    lower_n: 降频倍率, 默认2

rolling_average(): 滑动窗口平滑
    rolling_window: 滑动窗口大小, 默认2

kf_smooth(): 离线卡尔曼滤波平滑
    p_deviation: 转移过程的噪声标准差，默认0.01
    o_deviation: 观测过程的噪声标准差，默认0.1，o_deviation越小， 滤波平滑后的结果越接近观测轨迹(即源轨迹)
"""
import pandas as pd
from gotrackit.gps.Trajectory import TrajectoryPoints

if __name__ == '__main__':
    gps_df = pd.read_csv(r'gps.csv')

    # 去除同一出行中的相同定位时间点数据
    gps_df.drop_duplicates(subset=['agent_id', 'time'], keep='first', inplace=True)
    gps_df.reset_index(inplace=True, drop=True)

    # 构建TrajectoryPoints类, 并且指定一个plain_crs
    tp = TrajectoryPoints(gps_points_df=gps_df, time_unit='ms', plane_crs='EPSG:32649')

    # 间隔3个点采样一个点
    # tp.lower_frequency(lower_n=3)

    # 卡尔曼滤波平滑
    tp.kf_smooth()

    # 使用链式操作自定义预处理的先后顺序, 只要保证kf_smooth()操作后没有执行 - 滑动窗口平滑、增密，处理后的轨迹数据即可得到分项速度数据
    # tp.rolling_average().kf_smooth()
    # tp.rolling_average().lower_frequency().kf_smooth()

    # 获取清洗后的结果
    # _type参数可以取值为 df 或者 gdf
    process_df = tp.trajectory_data(_type='df')

    out_fldr = r'./data/output/'

    # 存储结果
    process_df.to_csv(os.path.join(out_fldr, r'after_reprocess_gps.csv'), encoding='utf_8_sig', index=False)

    # 输出为html进行动态可视化
    tp.export_html(out_fldr=out_fldr, file_name='sample')


"""
从GPS数据计算途径点OD
如果你的GPS数据已经完成了行程切分，且已经按照agent_id、time两个字段升序排列，那么你可以直接使用该接口进行途径点的抽样，得到带途径点的OD数据, 其数据格式满足 OD表要求
way_points_num
整数，OD的途径点数目，必须≤10，默认5个途径点
"""
import pandas as pd
from gotrackit.gps.GpsTrip import GpsPreProcess

if __name__ == '__main__':
    # 读取GPS数据
    gps_gdf = pd.read_csv(r'data/output/gps/example/gps_trip.cssv')

    # 新建一个GpsPreProcess示例
    grp = GpsPreProcess(gps_df=gps_gdf, use_multi_core=False)

    # 返回的第一个数据是OD表(pd.DataFrame)，第二个数据是OD线(gpd.GeoDataFrame)
    gps_od, od_line = grp.sampling_waypoints_od(way_points_num=2)
    gps_od.to_csv(r'./data/output/gps_od.csv', encoding='utf_8_sig', index=False)
    od_line.to_file(r'./data/output/gps_od.shp')


