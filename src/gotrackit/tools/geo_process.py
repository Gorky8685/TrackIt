# -- coding: utf-8 --
# @Time    : 2023/7/31 0031 9:52
# @Author  : TangKai
# @Team    : ZheChengData

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from .grid import get_grid_data
from ..GlobalVal import NetField
from .coord_trans import LngLatTransfer
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing

net_field = NetField()
geometry_field = net_field.GEOMETRY_FIELD
node_id_field = net_field.NODE_ID_FIELD
link_id_field = net_field.LINK_ID_FIELD
from_node_field = net_field.FROM_NODE_FIELD
to_node_field = net_field.TO_NODE_FIELD


def n_equal_points(n, from_loc: tuple = None, to_loc: tuple = None) -> list[tuple]:
    """

    :param n:
    :param from_loc:
    :param to_loc:
    :return:
    """
    assert n > 1
    try:
        equal_points = segmentize(s_loc=from_loc, e_loc=to_loc, n=n)
    except AttributeError:
        raise AttributeError(r'请升级geopandas到最新版本0.14.1')
    return equal_points


def cut_line_in_nearest_point(line, point) -> list[LineString]:
    """

    :param line:
    :param point:
    :return:
    """
    xd = line.project(point)
    return cut(line, xd)


def cut(line: LineString = None, dis: float = None) -> list[LineString]:
    """

    :param line:
    :param dis:
    :return:
    """
    if dis <= 0.0 or dis >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        xd = line.project(Point(p))
        if xd == dis:
            return [
                LineString(coords[:i + 1]),
                LineString(coords[i:])]
        if xd > dis:
            cp = line.interpolate(dis)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]


def calc_link_angle(link_geo1=None, link_geo2=None) -> float:
    """

    :param link_geo1:
    :param link_geo2:
    :return:
    """
    coord_list_a = list(link_geo1.coords)
    coord_list_b = list(link_geo2.coords)

    vec_a = np.array(coord_list_a[-1]) - np.array(coord_list_a[0])
    vec_b = np.array(coord_list_b[-1]) - np.array(coord_list_b[0])
    cos_res = np.dot(vec_a, vec_b) / (np.linalg.norm(vec_a) * np.linalg.norm(vec_b))

    if cos_res > 0:
        if cos_res >= 1:
            cos_res = 0.99999
        return 180 * np.arccos(cos_res) / np.pi
    elif cos_res < 0:
        if cos_res < -1.0:
            cos_res = -0.99999
    return 180 * np.arccos(cos_res) / np.pi


def segmentize(s_loc: list or tuple = None, e_loc: list or tuple = None, n: int = None) -> list[tuple]:
    """
    将直线对象line进行n等分加密, 返回中间的加密点坐标
    :param s_loc
    :param e_loc
    :param n:
    :return:
    """
    # s, e = coord_list[0], coord_list[-1]
    # try:
    #     k = (e_loc[1] - s_loc[1]) / (e_loc[0] - s_loc[0])
    # except ZeroDivisionError:
    #     gap = np.abs(e_loc[1] - s_loc[1]) / n
    #     return [(s_loc[0], s_loc[1] + (i + 1) * gap) for i in range(n - 1)]
    x_diff, y_diff = e_loc[0] - s_loc[0], e_loc[1] - s_loc[1]
    if np.abs(x_diff) <= 1e-5:
        gap = y_diff / n
        return [(s_loc[0], s_loc[1] + (i + 1) * gap) for i in range(n - 1)]
    else:
        k = y_diff / x_diff

    b = e_loc[1] - k * e_loc[0]
    gap_x = x_diff / n
    sample_x_list = [s_loc[0] + (i + 1) * gap_x for i in range(n - 1)]
    return [(sample_x, k * sample_x + b) for sample_x in sample_x_list]


def prj_inf(p: Point = None, line: LineString = None) -> tuple[Point, float, float, float, list[LineString], float, float]:
    """
    # 返回 某point到line的(投影点坐标, 点到投影点的直线距离, 投影点到line拓扑起点的路径距离, line的长度, 投影点打断line后的geo list)
    :param p:
    :param line:
    :return: (投影点坐标, 点到投影点的直线距离, 投影点到line拓扑起点的路径距离, line的长度, 投影点打断line后的geo list, 投影点的方向向量)
    """

    distance = line.project(p)

    if distance <= 0.0:
        line_cor = list(line.coords)
        prj_p = Point(line_cor[0])
        # prj_vec = np.array(line_cor[1]) - np.array(line_cor[0])
        dx, dy = line_cor[1][0] - line_cor[0][0], line_cor[1][1] - line_cor[0][1]
        return prj_p, prj_p.distance(p), distance, line.length, [LineString(line)], dx, dy
    elif distance >= line.length:
        line_cor = list(line.coords)
        prj_p = Point(line_cor[-1])
        # prj_vec = np.array(line_cor[-1]) - np.array(line_cor[-2])
        dx, dy = line_cor[-1][0] - line_cor[-2][0], line_cor[-1][1] - line_cor[-2][1]
        return prj_p, prj_p.distance(p), distance, line.length, [LineString(line)], dx, dy
    else:
        coords = list(line.coords)
        for i, _p in enumerate(coords):
            xd = line.project(Point(_p))
            if xd == distance:
                prj_p = Point(coords[i])
                # prj_vec = np.array(coords[i]) - np.array(coords[i - 1])
                dx, dy = coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]
                return prj_p, prj_p.distance(p), distance, line.length, \
                    [LineString(coords[:i + 1]), LineString(coords[i:])], dx, dy
            if xd > distance:
                cp = line.interpolate(distance)
                prj_p = Point((cp.x, cp.y))
                # prj_vec = np.array(coords[i]) - np.array(coords[i - 1])
                dx, dy = coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]
                return prj_p, prj_p.distance(p), distance, line.length, [LineString(coords[:i] + [(cp.x, cp.y)]),
                                                                         LineString(
                                                                             [(cp.x, cp.y)] + coords[i:])], dx, dy
    # to here means error
    raise ValueError(r'路段几何存在重叠环路线型, 请使用redivide_link_node函数处理')

def clean_link_geo(gdf: gpd.GeoDataFrame = None, plain_crs: str = 'EPSG:32650', l_threshold: float = 0.5) -> gpd.GeoDataFrame:
    """
    将geometry列中的Multi对象处理为single对象
    :param gdf:
    :param l_threshold:
    :param plain_crs
    :return:
    """
    assert geometry_field in gdf.columns
    origin_crs = gdf.crs.srs
    con = LngLatTransfer()

    gdf[geometry_field] = gdf.apply(lambda row: con.obj_convert(geo_obj=row[geometry_field], con_type='None'), axis=1)
    gdf = pd.DataFrame(gdf)
    gdf[[geometry_field, 'is_multi']] = \
        gdf.apply(lambda row:
                  (list(row[geometry_field].geoms), 1)
                  if isinstance(row[geometry_field], (MultiPolygon, MultiLineString, MultiPoint))
                  else (row[geometry_field], 0), axis=1, result_type='expand')

    is_multi_index = gdf['is_multi'] == 1
    multi_gdf = gdf[is_multi_index].copy()
    gdf.drop(index=gdf[is_multi_index].index, axis=0, inplace=True)

    multi_gdf = multi_gdf.explode(column=[geometry_field], ignore_index=True)
    multi_gdf.dropna(subset=[geometry_field], axis=0, inplace=True)

    gdf = pd.concat([gdf, multi_gdf])
    gdf.reset_index(inplace=True, drop=True)

    gdf.drop(columns=['is_multi'], axis=1, inplace=True)
    gdf = gpd.GeoDataFrame(gdf, geometry=geometry_field, crs=origin_crs)
    gdf = gdf.to_crs(plain_crs)
    gdf[geometry_field] = gdf[geometry_field].remove_repeated_points(l_threshold)
    gdf = gdf.to_crs(origin_crs)
    return gdf


def remapping_id(link_gdf: gpd.GeoDataFrame or pd.DataFrame = None,
                 node_gdf: gpd.GeoDataFrame or pd.DataFrame = None, start_link_id: int = 1,
                 start_node_id: int = 1) -> None:
    """
    change link and node inplace
    :param link_gdf:
    :param node_gdf:
    :param start_node_id
    :param start_link_id
    :return:
    """
    origin_node = set(node_gdf[node_id_field])
    node_map = {origin_node: new_node for origin_node, new_node in
                zip(origin_node, [i for i in range(start_node_id, len(origin_node) + start_node_id)])}

    node_gdf[node_id_field] = node_gdf[node_id_field].map(node_map)
    link_gdf[link_id_field] = [i for i in range(start_link_id, len(link_gdf) + start_link_id)]
    link_gdf[from_node_field] = link_gdf[from_node_field].map(node_map)
    link_gdf[to_node_field] = link_gdf[to_node_field].map(node_map)


def divide_line_by_l(line_geo: LineString = None, divide_l: float = 50.0, l_min: float = 0.5) -> \
        tuple[list[LineString], list[Point], int]:
    """

    :param line_geo:
    :param divide_l:
    :param l_min:
    :return:
    """
    line_l = line_geo.length
    n = int(line_l / divide_l)
    remain_l = line_l % divide_l
    is_remain = False
    if remain_l > l_min:
        is_remain = True
    p_list = [line_geo.interpolate(i * divide_l) for i in range(1, n + 1)]
    used_l = line_geo
    divide_line_list = []
    divide_point_list = []

    for i, p in enumerate(p_list):
        if i + 1 == len(p_list):
            if not is_remain:
                divide_line_list.append(used_l)
                break
        prj_p, _, dis, _, split_line_list, _, _ = prj_inf(p, used_l)
        used_l = split_line_list[-1]
        if i + 1 == len(p_list):
            if is_remain:
                divide_line_list.extend(split_line_list)
                divide_point_list.append(p)
                break
        divide_line_list.append(split_line_list[0])
        divide_point_list.append(p)

    return divide_line_list, divide_point_list, len(divide_line_list)


def vector_angle(v1: np.ndarray = None, v2: np.ndarray = None) -> float:
    # 计算点积
    dot_product = np.dot(v1, v2)
    # 计算两个向量的模
    norm_v1 = np.linalg.norm(v1)
    norm_v2 = np.linalg.norm(v2)

    # 避免除以零
    if norm_v1 == 0 or norm_v2 == 0:
        return 0
    # 计算夹角
    cos_angle = dot_product / (norm_v1 * norm_v2)

    # 防止因浮点数计算问题导致cos_angle超出范围
    cos_angle = min(max(cos_angle, -1), 1)

    # 计算弧度表示
    angle = np.arccos(cos_angle)
    return min(180 * angle / np.pi, 179.9)


def hmm_vector_angle(gps_diff_vec: np.ndarray = None, link_dir_vec: np.ndarray = None, omitted_l: float = 6.0) -> float:
    """
    计算GPS差分航向向量和候选路段切点方向向量的夹角
    :param gps_diff_vec:
    :param link_dir_vec:
    :param omitted_l
    :return:
    """
    # 在GPS点密集处, gps_diff_vec不准确, 往往gps_diff_vec的模会很小, 为了不干扰匹配, 返回0
    if np.sqrt(gps_diff_vec[0] ** 2 + gps_diff_vec[1] ** 2) <= omitted_l:
        return 0.0
    else:
        return vector_angle(v1=gps_diff_vec, v2=link_dir_vec)


def judge_plain_crs(lng: float = None) -> str:
    six_df = pd.DataFrame([(i * 6, 32631 + i) for i in range(-30, 30)],
                          columns=['start_lng', 'plain_crs'])
    res = six_df[lng - six_df['start_lng'] >= 0]['plain_crs'].max()
    return rf'EPSG:{res}'

def judge_plain_crs_based_on_node(node_gdf: gpd.GeoDataFrame = None) -> str:
    mean_x = np.array([geo.x for geo in node_gdf['geometry']]).mean()
    return judge_plain_crs(lng=mean_x)


def vec_angle(df: pd.DataFrame or gpd.GeoDataFrame = None, va_dx_field: str = 'gv_dx', va_dy_field: str = 'gv_dy',
              val_field: str = 'gvl',
              vb_dx_field: str = 'lv_dx', vb_dy_field: str = 'lv_dy', vbl_field: str = 'lvl'):
    """
    change inplace
    :param df:
    :param va_dx_field:
    :param va_dy_field:
    :param val_field:
    :param vb_dx_field:
    :param vb_dy_field:
    :param vbl_field:
    :return:
    """
    df['cos'] = (df[va_dx_field] * df[vb_dx_field] + df[va_dy_field] * df[vb_dy_field]) / (
                df[val_field] * df[vbl_field])
    df.loc[df['cos'] == -np.inf, 'cos'] = np.inf
    df.loc[df['cos'] <= -1, 'cos'] = -1
    df.loc[df['cos'] >= 1, 'cos'] = 1
    df['theta'] = 180 * np.arccos(df['cos']) / np.pi
    df.loc[df['theta'] >= 179.9, 'theta'] = 179.9


def rn_partition(region_gdf: gpd.GeoDataFrame = None, split_path_gdf: gpd.GeoDataFrame = None,
                 cpu_restrict: bool = True) -> gpd.GeoDataFrame:
    """传入面域, 对路网进行切块(依据region_gdf), 行不变, 加上新的一列region_id"""
    n = len(region_gdf)
    if cpu_restrict:
        assert n <= os.cpu_count(), f'区域数目:{n}, 大于当前CPU核数{os.cpu_count()}'
    region_gdf['region_id'] = [i for i in range(1, len(region_gdf) + 1)]
    if 'region_id' in split_path_gdf.columns:
        del split_path_gdf['region_id']
    split_path_gdf['index'] = [i for i in range(len(split_path_gdf))]
    split_path_gdf = gpd.sjoin(split_path_gdf, region_gdf[['region_id', net_field.GEOMETRY_FIELD]])
    if split_path_gdf.empty:
        return split_path_gdf
    split_path_gdf.sort_values(by=['index', 'region_id'], ascending=[True, True], inplace=True)
    split_path_gdf.drop_duplicates(subset=['index'], inplace=True, keep='first')
    del split_path_gdf['index_right'], split_path_gdf['index']
    return split_path_gdf


def rn_partition_alpha(split_path_gdf: gpd.GeoDataFrame = None, partition_num: int = 3,
                       is_geo_coord: bool = True):
    """传入面域, 对路网进行切块, 行不变, 加上新的一列region_id"""
    link_num = len(split_path_gdf)
    c_num = os.cpu_count()
    partition_num = c_num if partition_num > c_num else partition_num
    partition_len = int(link_num / partition_num) + 1
    _bound = split_path_gdf.bounds
    min_x, min_y, max_x, max_y = \
        _bound['minx'].min(), _bound['miny'].min(), _bound['maxx'].max(), _bound['maxy'].max()
    grid = get_grid_data(polygon_gdf=gpd.GeoDataFrame(geometry=[Polygon([(min_x, min_y), (max_x, min_y),
                                                                         (max_x, max_y),
                                                                         (min_x, max_y)])],
                                                      crs=split_path_gdf.crs), meter_step=2000,
                         is_geo_coord=is_geo_coord)

    # add region_id
    split_path_gdf = rn_partition(split_path_gdf=split_path_gdf, region_gdf=grid, cpu_restrict=False)
    if split_path_gdf.empty:
        return split_path_gdf
    split_path_gdf.sort_values(by='region_id', ascending=True, inplace=True)
    grid_count = split_path_gdf.groupby('region_id')[['geometry']].count().rename(
        columns={'geometry': 'count'}).reset_index(drop=False)
    grid_count['label'] = (grid_count['count'].cumsum() / partition_len).astype(int)
    region_group_map = {r: g for r, g in zip(grid_count['region_id'], grid_count['label'])}
    split_path_gdf['region_id'] = split_path_gdf['region_id'].map(region_group_map)
    return split_path_gdf


if __name__ == '__main__':
    pass
    # import geopandas as gpd
    # import matplotlib.pyplot as plt
    #
    # l = LineString([(0,0), (12, 90)])
    # p = gpd.GeoDataFrame({'geometry': [Point(item) for item in l.coords]}, geometry='geometry')
    # p.plot()
    # plt.show()
    #
    # l_1 = segmentize(line=l, n=12)
    # print(len(list(l_1.coords)))
    # p1 = gpd.GeoDataFrame({'geometry': [Point(item) for item in l_1.coords]}, geometry='geometry')
    # p1.plot()
    # plt.show()




