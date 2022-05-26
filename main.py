def declare(name, file):
    result = "  {}:\n".format(name) +\
             "    container_name: {}\n".format(name) +\
             "    image: {}:latest\n".format(file) +\
             "    entrypoint: python3 /{}.py\n".format(file)

    return result


end = "    restart: on-failure\n" +\
      "    links:\n" +\
      "      - rabbitmq\n" +\
      "    depends_on:\n" +\
      "      - rabbitmq\n\n"


class Reader:
    def __init__(self):
        with open("./config.txt") as file:
            lines = file.readlines()
            self.number_of_rows = int(lines[0].split(":")[1])
            self.number_of_adders = int(lines[1].split(":")[1])
            self.number_of_post_dispatchers = int(lines[2].split(":")[1])
            self.number_of_student_comments = int(lines[3].split(":")[1])
            self.number_of_joiners = int(lines[4].split(":")[1])
            self.number_of_join_avgs = int(lines[5].split(":")[1])
            self.number_of_sentiment_adders = int(lines[6].split(":")[1])
            file.close()

    def make_docker(self):
        file_info = "version: '3'\n" +\
                    "services:\n" +\
                    "  rabbitmq:\n" +\
                    "    container_name: rabbitmq\n" +\
                    "    image: rabbitmq:latest\n" +\
                    "    ports:\n" +\
                    "      - 15672:15672\n\n"

        file_info = file_info +\
                    declare("post_receiver", "post_receiver") +\
                    "    environment:\n" +\
                    "      - PYTHONUNBUFFERED=1\n" +\
                    "      - PYTHONHASHSEED=0\n" +\
                    "      - ADDERS={}\n".format(self.number_of_adders) +\
                    "      - DISPATCHERS={}\n".format(self.number_of_post_dispatchers) +\
                    end

        file_info = file_info +\
                    declare("comments_receiver", "comments_receiver") +\
                    "    environment:\n" +\
                    "      - PYTHONUNBUFFERED=1\n" +\
                    "      - PYTHONHASHSEED=0\n" +\
                    "      - STUDENT_COMM={}\n".format(self.number_of_student_comments) +\
                    end

        file_info = file_info + \
                    declare("client", "client") + \
                    "    environment:\n" + \
                    "      - PYTHONUNBUFFERED=1\n" + \
                    "      - PYTHONHASHSEED=0\n" + \
                    "      - NUMBER_OF_ROW={}\n".format(self.number_of_rows) + \
                    "      - AVG_JOINER={}\n".format(self.number_of_join_avgs) + \
                    "    volumes:\n" +\
                    "      - ./CSV/:/CSV/\n" +\
                    end

        for i in range(self.number_of_adders):
            file_info = file_info + \
                        declare("adder{}".format(i), "adder") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - ADDER_NUM={}\n".format(i) + \
                        end

        for i in range(self.number_of_joiners):
            file_info = file_info + \
                        declare("joiner{}".format(i), "joiner") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - JOINER_NUM={}\n".format(i) + \
                        "      - SENT_ADDER={}\n".format(self.number_of_sentiment_adders) + \
                        "      - AVG_JOINER={}\n".format(self.number_of_join_avgs) + \
                        "      - POST_DISPATCHERS={}\n".format(self.number_of_post_dispatchers) + \
                        "      - STUDENT_COMM={}\n".format(self.number_of_student_comments) + \
                        "      - NUMBER_OF_ROW={}\n".format(self.number_of_rows) + \
                        end

        for i in range(self.number_of_sentiment_adders):
            file_info = file_info + \
                        declare("sent_adder{}".format(i), "sentiment_adder") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - ADDER_NUM={}\n".format(i) + \
                        "      - JOINERS={}\n".format(self.number_of_joiners) + \
                        end

        for i in range(self.number_of_post_dispatchers):
            file_info = file_info + \
                        declare("post_disp{}".format(i), "post_join_dispatch") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - DISPATCH_NUM={}\n".format(i) + \
                        "      - JOINER={}\n".format(self.number_of_joiners) + \
                        end

        file_info = file_info + \
                    declare("top_sentiment", "top_sentiment") + \
                    "    environment:\n" + \
                    "      - PYTHONUNBUFFERED=1\n" + \
                    "      - PYTHONHASHSEED=0\n" + \
                    "      - SENT_ADDERS={}\n".format(self.number_of_sentiment_adders) + \
                    end

        file_info = file_info + \
                    declare("avg", "avg") + \
                    "    environment:\n" + \
                    "      - PYTHONUNBUFFERED=1\n" + \
                    "      - PYTHONHASHSEED=0\n" + \
                    "      - ADDERS={}\n".format(self.number_of_adders) + \
                    "      - AVG_JOINER={}\n".format(self.number_of_join_avgs) + \
                    end

        for i in range(self.number_of_join_avgs):
            file_info = file_info + \
                        declare("join_avg{}".format(i), "join_avg") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - JOINER_NUM={}\n".format(i) + \
                        "      - JOINERS={}\n".format(self.number_of_joiners) + \
                        end

        for i in range(self.number_of_student_comments):
            file_info = file_info + \
                        declare("student_comment{}".format(i), "student_comment") + \
                        "    environment:\n" + \
                        "      - PYTHONUNBUFFERED=1\n" + \
                        "      - PYTHONHASHSEED=0\n" + \
                        "      - INSTANCE_NUM={}\n".format(i) + \
                        "      - JOINER={}\n".format(self.number_of_joiners) + \
                        end

        with open("./docker-compose-dev.yaml", 'w') as file:
            file.write(file_info)
            file.close()


if __name__ == '__main__':
    reader = Reader()
    reader.make_docker()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
