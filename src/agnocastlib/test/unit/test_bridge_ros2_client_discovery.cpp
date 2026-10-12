#include "agnocast/bridge/agnocast_bridge_utils.hpp"

#include <rclcpp/rclcpp.hpp>

#include <rclcpp/version.h>

#if RCLCPP_VERSION_MAJOR < 28

#include <rcl_interfaces/srv/get_parameters.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <thread>

namespace
{

using namespace std::chrono_literals;  // NOLINT(build/namespaces)

constexpr const char * PEER_SERVICE = "/agnocast_collect_test/peer_service";
constexpr const char * OWN_SERVICE = "/agnocast_collect_test/own_service";
constexpr const char * NAMESAKE_SERVICE = "/agnocast_collect_test/namesake_service";

// Graph updates are asynchronous. Other nodes may share the ROS domain, so the tests below only
// ever assert on the service names they own.
std::set<std::string> collect_until_visible(rclcpp::Node * container, const std::string & service)
{
  const auto deadline = std::chrono::steady_clock::now() + 10s;

  while (true) {
    auto names = agnocast::collect_external_ros2_client_service_names(container);
    if (names.count(service) > 0 || std::chrono::steady_clock::now() >= deadline) {
      return names;
    }
    std::this_thread::sleep_for(50ms);
  }
}

class CollectExternalRos2ClientServiceNamesTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    rclcpp::init(0, nullptr);
    container_ = std::make_shared<rclcpp::Node>("agnocast_collect_test_container");
    peer_ = std::make_shared<rclcpp::Node>("agnocast_collect_test_peer");
  }

  void TearDown() override
  {
    peer_.reset();
    container_.reset();
    rclcpp::shutdown();
  }

  rclcpp::Node::SharedPtr container_;
  rclcpp::Node::SharedPtr peer_;
};

TEST_F(CollectExternalRos2ClientServiceNamesTest, skips_a_client_held_by_the_given_node)
{
  // The peer's client is the synchronization point: once it shows up, discovery has run, so the
  // container's own client being absent is a real exclusion rather than a not-yet-discovered one.
  auto peer_client = peer_->create_client<rcl_interfaces::srv::GetParameters>(PEER_SERVICE);
  auto own_client = container_->create_client<rcl_interfaces::srv::GetParameters>(OWN_SERVICE);

  const auto names = collect_until_visible(container_.get(), PEER_SERVICE);

  ASSERT_EQ(names.count(PEER_SERVICE), 1U);
  EXPECT_EQ(names.count(OWN_SERVICE), 0U);
}

TEST_F(
  CollectExternalRos2ClientServiceNamesTest,
  reports_a_client_held_by_a_namesake_in_another_namespace)
{
  auto namesake = std::make_shared<rclcpp::Node>(
    "agnocast_collect_test_container", "/agnocast_collect_test_other");
  auto namesake_client =
    namesake->create_client<rcl_interfaces::srv::GetParameters>(NAMESAKE_SERVICE);

  EXPECT_EQ(collect_until_visible(container_.get(), NAMESAKE_SERVICE).count(NAMESAKE_SERVICE), 1U);
}

TEST_F(CollectExternalRos2ClientServiceNamesTest, returns_empty_for_a_null_node)
{
  EXPECT_TRUE(agnocast::collect_external_ros2_client_service_names(nullptr).empty());
}

}  // namespace

#endif
