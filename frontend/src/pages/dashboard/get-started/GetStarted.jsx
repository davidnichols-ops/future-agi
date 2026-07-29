import React from "react";
import { Helmet } from "react-helmet-async";
import GetStartedView from "src/sections/get-started/GetStartedView";
import GetStartedOssView from "src/sections/get-started/GetStartedOssView";
import { useDeploymentMode } from "src/hooks/useDeploymentMode";

const GetStarted = () => {
  const { isOSS } = useDeploymentMode();
  return (
    <>
      <Helmet>
        <title>Get started with FutureAGI</title>
      </Helmet>
      {isOSS ? <GetStartedOssView /> : <GetStartedView />}
    </>
  );
};

export default GetStarted;
