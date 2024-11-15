from bs4 import BeautifulSoup
import requests

def main():
    print("Hello")
    URL = "https://catalog.rpi.edu/content.php?catoid=30&navoid=864"
    storage_p = []
    storage_ul = []
    pageToScrape = requests.get(URL)
    while True:
        if pageToScrape.status_code == 200:
            soup = BeautifulSoup(pageToScrape.text, 'html.parser')
            portfolios = soup.find('td', attrs={'class':'block_content', 'colspan':'2'})

            # get all the degree names
            p_names = portfolios.findAll('p', attrs={'style':'padding-left: 30px'})
            for name in p_names:
                strong = name.find('strong').get_text()
                storage_p.append(strong)

            
            # get all the degree links and names of degrees
            degree_links = portfolios.findAll('ul', attrs={'class':'program-list'})
            for degree_type in degree_links:
                current_degrees = []
                list_degrees = degree_type.findAll('li', attrs={'style':'list-style-type: none'})
                for degree in list_degrees:
                    href = "https://catalog.rpi.edu/" + degree.find('a').get('href')
                    name = degree.find('a').get_text()
                    name_link_pair = [name, href]
                    current_degrees.append(name_link_pair)
                storage_ul.append(current_degrees)
        
            # - All the degrees in each type
            # print(storage_ul)
            # - Type of degrees
            # print(storage_p)
            # time to visit individual links and find the credit requirements!
            # use "acalog index % 3" to get the year, fall semester, and spring semester. 
    
            # more issues:
            # 1. Not all the pages are in the same format. You will indeed have a total of 8 acalog_core's and 4 headers per undergraduate degree (check COMD for the error)
            # 2. Try using a counter instead of intentionally shortening the acalog_core list. The code will need a lot of work.
            classes_and_requirements = {}
            for index_of_degree in range(len(storage_p)):
                # print(storage_ul[index_of_degree])
                # bachelors
                if storage_p[index_of_degree] == "Masters":
                    print("Hello")
                    if i == 0:
                                    index = 0
                                    while index < len(fall_classes):
                                        class_and_credits = fall_classes[index].get_text()
                                        try:
                                            test = class_and_credits.index(":")
                                            # Proceed with the logic if the colon is found
                                            # For example, split the string
                                            class_item = class_and_credits[0:test - 13].replace("….", "... -")
                                            credits_per_class = class_and_credits[test + 2:test + 3]
                                            fall_sem.append(class_item + ":" + str(credits_per_class))
                                            index += 1
                                        except ValueError:
                                            # Skip the item or handle it in case of missing colon
                                            # print("Colon not found, skipping this entry.")
                                            if class_and_credits == "or":
                                                or_classes = []
                                                if len(fall_sem) > 0:
                                                    fall_sem.pop(len(fall_sem) - 1)
                                                first_choice = fall_classes[index - 1].get_text()

                                                test = first_choice.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = first_choice[0:test - 13]
                                                credits_per_class = first_choice[test + 2:test + 3]
                                                or_classes.append(class_item + ":" + str(credits_per_class))

                                                second_choice = fall_classes[index + 1].get_text()
                                                test = second_choice.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = second_choice[0:test]
                                                credits_per_class = second_choice[test + 2:test + 3]
                                                or_classes.append(class_item + ":" + str(credits_per_class))
                                                fall_sem.append(or_classes)
                                                index += 2
                                            else:
                                                index += 1

                                    # do the same thing with spring classes
                                    index = 0
                                    while index < len(spring_classes):
                                        class_and_credits = spring_classes[index].get_text()
                                        try:
                                            test = class_and_credits.index(":")
                                            # Proceed with the logic if the colon is found
                                            # For example, split the string
                                            class_item = class_and_credits[0:test - 13].replace("….", "... -")
                                            credits_per_class = class_and_credits[test + 2:test + 3]
                                            spring_sem.append(class_item + ":" + str(credits_per_class))
                                            index += 1
                                        except ValueError:
                                            # Skip the item or handle it in case of missing colon
                                            # print("Colon not found, skipping this entry.")
                                            if class_and_credits == "or":
                                                or_classes = []
                                                if len(spring_sem) > 0:
                                                    spring_sem.pop(len(spring_sem) - 1)
                                                first_choice = spring_classes[index - 1].get_text()

                                                test = first_choice.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = first_choice[0:test - 13]
                                                credits_per_class = first_choice[test + 2:test + 3]
                                                or_classes.append(class_item + ":" + str(credits_per_class))

                                                second_choice = spring_classes[index + 1].get_text()
                                                test = second_choice.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = second_choice[0:test]
                                                credits_per_class = second_choice[test + 2:test + 3]
                                                or_classes.append(class_item + ":" + str(credits_per_class))
                                                spring_sem.append(or_classes)
                                                index += 2
                                            else:
                                                index += 1
                                    # add all the courses into the requirements
                                    classes_and_requirements[degree[0]]["First Year"]["Fall"] = fall_sem
                                    classes_and_requirements[degree[0]]["First Year"]["Spring"] = spring_sem
                                    # print(classes_and_requirements)



if __name__ == "__main__":
    main()