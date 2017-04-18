dockerNode(image: 'uf-mil:txros') {
	stage("Checkout") {
		checkout scm
		sh '''
			ln -s `pwd` ~/catkin_ws/src/txros
			git submodule update --init --recursive
		'''
	}
	stage("Build") {
		sh '''
			source /opt/ros/kinetic/setup.bash > /dev/null 2>&1
			catkin_make -C ~/catkin_ws -B
		'''
	}
	stage("Test") {
		sh '''
			source /opt/ros/kinetic/setup.bash > /dev/null 2>&1
			source ~/catkin_ws/devel/setup.bash > /dev/null 2>&1
			trial txros
		'''
	}
}
