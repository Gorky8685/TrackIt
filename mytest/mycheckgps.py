import pandas as pd
import ast

# 输入文件路径
input_file = 'F:\github_desktop_code\TrackIt\data\output\公交轨迹数据.csv'  # 替换为你的公交轨迹数据文件路径
output_file = 'F:\github_desktop_code\TrackIt\data\output\processed_公交轨迹数据.csv'  # 替换为你想要保存的输出文件路径

# 读取公交轨迹数据
bus_data = pd.read_csv(input_file)

# 定义函数来解析new_content列，并将经纬度分开
def process_new_content(content):
    try:
        # 将字符串格式的列表转换为真正的列表
        content_list = ast.literal_eval(content)
        # 将经纬度分开处理，并转为浮点数
        lng_lat_list = [tuple(map(float, item.split(','))) for item in content_list]
        return lng_lat_list
    except (ValueError, SyntaxError):
        return []

# 处理new_content列，获取经纬度对
bus_data['lng_lat'] = bus_data['new_content'].apply(process_new_content)

# 拆分经纬度，并为每个经纬度对创建新的行
expanded_data = pd.DataFrame(
    [(row['id'], lng, lat) for _, row in bus_data.iterrows() for lng, lat in row['lng_lat']],
    columns=['agent_id', 'lng', 'lat']
)

# 设置初始时间
start_time = pd.to_datetime("2024-09-06 00:00:00", format="%Y-%m-%d %H:%M:%S")

# 按照每个agent_id生成递增的时间，每行加3分钟
expanded_data['time'] = expanded_data.groupby('agent_id').cumcount() * 20
expanded_data['time'] = expanded_data['time'].apply(lambda x: start_time + pd.Timedelta(seconds=x))

# 按需求顺序排列列
expanded_data = expanded_data[['time', 'agent_id', 'lng', 'lat']]

# 将结果保存到CSV文件中
expanded_data.to_csv(output_file, index=False)

print(f"处理完成，文件已保存至: {output_file}")

