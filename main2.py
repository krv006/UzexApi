import requests
import csv

file = open("tasnif_soliq_data.csv", 'wt', encoding='utf-8', newline='')
writer = csv.writer(file)
writer.writerow(['Guruh', 'Sinf', 'Pozitsiya','Subpozitsiya'])

base_url = "https://tasnif.soliq.uz/api/cls-api/"
group_url = "group?size=200&lang=uz_cyrl"
class_url = "class/short-info?groupCode={group_id}&lang=uz_cyrl&size=20"
position_url = "position/short-info?classCode={class_id}&lang=uz_cyrl&size=20"
subposition_url = "subposition/short-info?positionCode={position_code}&lang=uz_cyrl&size=20"

res_group = requests.get(base_url +group_url)
res_group.raise_for_status()
res_group_data = res_group.json()['data']



for group_info in res_group_data:
    res_class = requests.get(base_url + class_url.format(group_id=group_info['code']))
    res_class.raise_for_status()
    res_class_data = res_class.json()['data']


    for class_info in res_class_data:
        # print("Class Info:", class_info)
        res_position = requests.get(base_url+position_url.format(class_id=class_info['code']))
        res_position.raise_for_status()
        res_position_data = res_position.json()['data']

        for position_info in res_position_data:
            # print("Position Info:", position_info)
            res_subposition = requests.get(base_url+subposition_url.format(position_code=position_info['code']))
            res_subposition.raise_for_status()
            res_subposition_data = res_subposition.json()['data']

            for subposition_info in res_subposition_data:
                group_name = f"{group_info['code']} - {group_info['name']}"
                class_name = f"{class_info['code']} - {class_info['name']}" 
                position_name = f"{position_info['code']} - {position_info['name']}"
                subposition_name = f"{subposition_info['code']} - {subposition_info['name']}"
                writer.writerow([group_name,class_name,position_name,subposition_name])

file.close()